# 依赖在 composer 镜像里安装（旧的 php:7.3 镜像基于已归档的 Debian，没有 unzip）。
FROM composer:2.2 AS vendor
WORKDIR /app
RUN composer require --no-interaction --no-progress --ignore-platform-reqs php-amqplib/php-amqplib:3.1.0

FROM php:7.3-cli
RUN docker-php-ext-install sockets bcmath >/dev/null
WORKDIR /app
COPY --from=vendor /app/vendor /app/vendor
COPY publisher.php consumer.php /app/
