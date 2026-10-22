#!/usr/bin/env python3
"""内存验收压测（issue #13）。

在本机运行代理，用 Docker 里的 PHP 7.3 + php-amqplib 3.1.0 产生负载：
  * 模拟 PHP-FPM 的短连接发布者（连接 → open channel → publish → close）
  * 常驻 CLI 消费者（basic_consume + wait，heartbeat=0，keepalive=false）
  * 混入：随机杀掉容器、RabbitMQ 重启、错误密码、不发 close 就断开

每 10 秒记录代理的 RSS / 线程 / 文件描述符 / 连接数，输出 CSV 和中文报告，
并按验收标准给出通过或失败（退出码 0 / 1）。

用法：
    test/soak/build.sh                       # 构建 musl + jemalloc 二进制
    test/soak/soak.py --mode smoke           # 约 15 分钟
    test/soak/soak.py --mode accelerated     # 约 2 小时（默认）
    test/soak/soak.py --duration 3600 --publishers 6   # 自定义

需要 Docker，以及 test/docker-compose.yml 里的 RabbitMQ。
"""

import argparse
import csv
import os
import pathlib
import random
import re
import statistics
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "test" / "docker-compose.yml"
IMAGE = "amqproxy-soak-php"
RABBIT = "test-rabbitmq-1"
PROXY_PORT = 5673
IDLE_TIMEOUT = 5
SAMPLE_EVERY = 10

MODES = {
    # 时长(秒)、发布者数、消费者数、重启 RabbitMQ 的间隔、杀容器的间隔
    "smoke": dict(duration=900, publishers=4, consumers=2, restart=300, kill=60),
    "accelerated": dict(duration=7200, publishers=8, consumers=4, restart=600, kill=30),
}

STATS_RE = re.compile(r"clients=(\d+) upstreams=(\d+) rss=(\S+?)MB")


def sh(*cmd, check=True, timeout=None):
    return subprocess.run(cmd, check=check, capture_output=True, text=True, timeout=timeout)


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


class Proxy:
    def __init__(self, binary, out_dir):
        self.log_path = out_dir / "proxy.log"
        self.log_file = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [str(binary), "-l", "127.0.0.1", "-p", str(PROXY_PORT), "-t", str(IDLE_TIMEOUT),
             "amqp://127.0.0.1:5672"],
            stdout=self.log_file, stderr=subprocess.STDOUT,
        )
        self.pid = self.proc.pid
        self.last_stats = (None, None)

    def alive(self):
        return self.proc.poll() is None

    def proc_stat(self):
        rss = threads = None
        with open(f"/proc/{self.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("Threads:"):
                    threads = int(line.split()[1])
        fds = len(os.listdir(f"/proc/{self.pid}/fd"))
        return rss, threads, fds

    def stats(self):
        """日志里最新的 (clients, upstreams)。"""
        try:
            text = self.log_path.read_text()
        except OSError:
            return self.last_stats
        found = STATS_RE.findall(text[-4096:])
        if found:
            self.last_stats = (int(found[-1][0]), int(found[-1][1]))
        return self.last_stats

    def stop(self):
        if self.alive():
            self.proc.terminate()
            try:
                self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log_file.close()


def rabbit_connections():
    r = sh("docker", "exec", RABBIT, "rabbitmqctl", "list_connections", "-q", "--silent",
           check=False, timeout=30)
    if r.returncode != 0:
        return None
    return len([line for line in r.stdout.splitlines() if line.strip()])


def wait_rabbit(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = sh("docker", "exec", RABBIT, "rabbitmq-diagnostics", "-q", "ping", check=False, timeout=30)
        if r.returncode == 0:
            return
        time.sleep(2)
    raise RuntimeError("RabbitMQ did not become healthy")


def start_container(name, script, env=None):
    sh("docker", "rm", "-f", name, check=False)
    args = ["docker", "run", "-d", "--name", name, "--network", "host"]
    for k, v in (env or {}).items():
        args += ["-e", f"{k}={v}"]
    args += [IMAGE, "php", script]
    sh(*args)


def cleanup_containers():
    names = sh("docker", "ps", "-aq", "--filter", "name=soak-", check=False).stdout.split()
    if names:
        sh("docker", "rm", "-f", *names, check=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=MODES, default="accelerated")
    ap.add_argument("--duration", type=int, help="负载时长（秒）")
    ap.add_argument("--publishers", type=int)
    ap.add_argument("--consumers", type=int)
    ap.add_argument("--restart-interval", type=int, help="重启 RabbitMQ 的间隔（秒），0 表示不重启")
    ap.add_argument("--kill-interval", type=int, help="随机杀容器的间隔（秒），0 表示不杀")
    ap.add_argument("--binary", default=str(ROOT / "target" / "soak-build" / "release" / "amqproxy"))
    ap.add_argument("--out", help="输出目录，默认 target/soak/<时间>")
    args = ap.parse_args()

    cfg = dict(MODES[args.mode])
    for key, val in (("duration", args.duration), ("publishers", args.publishers),
                     ("consumers", args.consumers), ("restart", args.restart_interval),
                     ("kill", args.kill_interval)):
        if val is not None:
            cfg[key] = val
    out = pathlib.Path(args.out) if args.out else ROOT / "target" / "soak" / time.strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    binary = pathlib.Path(args.binary)
    if not binary.exists():
        sys.exit(f"找不到 {binary}。先运行 test/soak/build.sh。")

    log(f"配置: {cfg}，输出目录: {out}")
    sh("test/gen-certs.sh", check=False)
    sh("docker", "compose", "-f", str(COMPOSE), "up", "-d", "--wait")
    sh("docker", "build", "-q", "-t", IMAGE, str(ROOT / "test" / "soak" / "php"))
    cleanup_containers()

    proxy = Proxy(binary, out)
    samples = []
    csv_file = open(out / "samples.csv", "w", newline="")
    writer = csv.writer(csv_file)
    writer.writerow(["elapsed_s", "phase", "rss_kb", "threads", "fds", "clients", "upstreams", "rabbit_connections"])
    t0 = time.time()
    rabbit_conns = None
    next_conn_check = 0

    def sample(phase):
        nonlocal rabbit_conns, next_conn_check
        if not proxy.alive():
            raise RuntimeError("代理进程已退出！见 proxy.log")
        rss, threads, fds = proxy.proc_stat()
        clients, upstreams = proxy.stats()
        now = time.time() - t0
        if now >= next_conn_check:
            rabbit_conns = rabbit_connections()
            next_conn_check = now + 60
        row = [round(now), phase, rss, threads, fds, clients, upstreams, rabbit_conns]
        writer.writerow(row)
        csv_file.flush()
        samples.append(dict(t=now, phase=phase, rss=rss, threads=threads, fds=fds,
                            clients=clients, upstreams=upstreams, rabbit=rabbit_conns))
        return samples[-1]

    failed = None
    try:
        # 1. 空闲基线
        log("空闲基线 (30 秒)…")
        for _ in range(3):
            time.sleep(SAMPLE_EVERY)
            sample("idle0")

        # 2. 启动负载
        log(f"启动 {cfg['consumers']} 个消费者和 {cfg['publishers']} 个发布者")
        for i in range(cfg["consumers"]):
            start_container(f"soak-con-{i}", "consumer.php")
        time.sleep(3)
        for i in range(cfg["publishers"]):
            start_container(f"soak-pub-{i}", "publisher.php")

        load_start = time.time()
        next_restart = load_start + cfg["restart"] if cfg["restart"] else None
        next_kill = load_start + cfg["kill"] if cfg["kill"] else None
        next_badauth = load_start + 120
        next_sample = load_start
        while time.time() - load_start < cfg["duration"]:
            now = time.time()
            if now >= next_sample:
                s = sample("load")
                if s["rss"] and len(samples) % 6 == 0:
                    log(f"{round(now - load_start)}s  rss={s['rss'] // 1024}MB clients={s['clients']} "
                        f"upstreams={s['upstreams']} fds={s['fds']}")
                next_sample += SAMPLE_EVERY
            if next_kill and now >= next_kill:
                victim = random.choice([f"soak-pub-{i}" for i in range(cfg["publishers"])] * 3
                                       + [f"soak-con-{i}" for i in range(cfg["consumers"])])
                log(f"杀掉 {victim}")
                sh("docker", "kill", victim, check=False)
                sh("docker", "start", victim, check=False)
                next_kill = now + cfg["kill"]
            if next_restart and now >= next_restart:
                log("重启 RabbitMQ")
                sh("docker", "restart", RABBIT, timeout=180)
                wait_rabbit()
                log("RabbitMQ 已恢复")
                next_restart = time.time() + cfg["restart"]
            if now >= next_badauth:
                # 错误密码的客户端持续 5 秒
                sh("docker", "run", "--rm", "--network", "host", "-e", "AMQP_PASS=wrong", IMAGE,
                   "timeout", "5", "php", "publisher.php", check=False)
                next_badauth = time.time() + 120
            time.sleep(0.5)

        # 3. 停止负载，等待回收
        log("停止负载，等待空闲回收…")
        cleanup_containers()
        time.sleep(IDLE_TIMEOUT + 10)
        end_load = time.time() - t0
        for _ in range(9):
            sample("idle1")
            time.sleep(SAMPLE_EVERY)
        sample("idle1")
    except Exception as e:  # noqa: BLE001
        failed = str(e)
        log(f"出错: {e}")
    finally:
        cleanup_containers()
        csv_file.close()
        proxy.stop()

    report(out, cfg, samples, failed)


def mean(xs):
    return sum(xs) / len(xs)


def report(out, cfg, samples, failed):
    idle0 = [s for s in samples if s["phase"] == "idle0"]
    load = [s for s in samples if s["phase"] == "load"]
    idle1 = [s for s in samples if s["phase"] == "idle1"]
    lines = ["# 内存验收报告", ""]
    checks = []

    if failed:
        checks.append(("压测正常完成", False, failed))
    if not (idle0 and load and idle1):
        checks.append(("样本完整", False, "缺少某个阶段的样本"))
    else:
        base_rss = statistics.median(s["rss"] for s in idle0) / 1024
        base_fds = statistics.median(s["fds"] for s in idle0)
        warmup = max(180, cfg["duration"] * 0.1)
        stable = [s for s in load if s["t"] - load[0]["t"] >= warmup]
        n = max(1, len(stable) // 5)
        if len(stable) >= 2 * n:
            a = mean([s["rss"] for s in stable[:n]]) / 1024
            b = mean([s["rss"] for s in stable[-n:]]) / 1024
            growth_mb = b - a
            growth_pct = growth_mb / a * 100
            ok = growth_pct < 5 or growth_mb < 2
            checks.append((
                "稳定阶段 RSS 增长 < 5%（或 < 2 MB）", ok,
                f"预热 {int(warmup)} 秒后：前 20% 平均 {a:.1f} MB → 后 20% 平均 {b:.1f} MB，"
                f"增长 {growth_mb:+.1f} MB ({growth_pct:+.1f}%)"))
        else:
            checks.append(("稳定阶段 RSS 增长 < 5%", False, "稳定阶段样本太少，请增加 --duration"))

        last = idle1[-3:]
        final_rss = statistics.median(s["rss"] for s in last) / 1024
        final_fds = statistics.median(s["fds"] for s in last)
        checks.append(("空闲后 clients=0、upstreams=0",
                       all(s["clients"] == 0 and s["upstreams"] == 0 for s in last),
                       f"最后几个样本：{[(s['clients'], s['upstreams']) for s in last]}"))
        checks.append(("空闲后 RSS 回到基线附近（基线 + 30 MB 以内）", final_rss <= base_rss + 30,
                       f"基线 {base_rss:.1f} MB，负载期间峰值 {max(s['rss'] for s in load) / 1024:.1f} MB，"
                       f"空闲后 {final_rss:.1f} MB"))
        checks.append(("空闲后文件描述符回到基线（+5 以内）", final_fds <= base_fds + 5,
                       f"基线 {base_fds:.0f}，空闲后 {final_fds:.0f}"))
        rabbit = [s["rabbit"] for s in last if s["rabbit"] is not None]
        if rabbit:
            checks.append(("空闲后 RabbitMQ 上没有残留的代理连接", rabbit[-1] == 0, f"连接数 {rabbit[-1]}"))
        lines += [f"- 负载时长：{cfg['duration']} 秒；发布者 {cfg['publishers']}，消费者 {cfg['consumers']}",
                  f"- 线程数：基线 {idle0[-1]['threads']}，负载期间最大 {max(s['threads'] for s in load)}",
                  f"- 文件描述符：负载期间最大 {max(s['fds'] for s in load)}", ""]

    passed = all(ok for _, ok, _ in checks)
    lines += ["| 检查项 | 结果 | 详情 |", "|---|---|---|"]
    for name, ok, detail in checks:
        lines.append(f"| {name} | {'通过' if ok else '**失败**'} | {detail} |")
    lines += ["", f"## 结论：{'通过' if passed else '失败'}", "", "原始数据：`samples.csv`；代理日志：`proxy.log`。"]
    text = "\n".join(lines) + "\n"
    (out / "report.md").write_text(text)
    print("\n" + text)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
