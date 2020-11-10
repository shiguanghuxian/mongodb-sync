FROM alpine:latest
# 复制可执行程序
COPY ./bin/mongodb-sync /data/
# 需要挂载目录
VOLUME /data/logs
VOLUME /data/oplog
VOLUME /data/config/cfg.toml
# 工作目录
WORKDIR /data/
# 启动脚本
CMD ["./mongodb-sync"]
