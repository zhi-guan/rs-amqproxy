# musl 静态二进制 + jemalloc

部署目标包括 CentOS 7.4（glibc 2.17）和 Debian 12。我们编译为 `x86_64-unknown-linux-musl` 静态二进制，这样一个文件就能在两种系统上运行，不依赖系统 glibc。

musl 自带的 malloc 速度慢，而且长时间运行时容易产生碎片，RSS 看起来会像在"泄露"。因此我们使用 jemalloc（`tikv-jemallocator`），开启后台线程，把空闲内存归还给系统。jemalloc 的统计接口也可以用来做内存验收。

## 考虑过的方案

- **mimalloc**：也可以使用，但 jemalloc 在长时间运行的网络服务中经验更多，而且统计接口更完善。
- **glibc 动态链接**：需要在 CentOS 7 上编译，或者在每个系统上分别编译，维护成本更高。
