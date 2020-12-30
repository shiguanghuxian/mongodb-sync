# Mongodb Sync
用于监听mongodb数据变化同步数据到备份数据库

当前支持同步数据到mongodb、elasticsearch、文件、mysql



## TODO
- 支持初始化时同步全部数据
- 源数据字段为M D 等map或切片时的处理
- 增加插入前数据处理钩子
- 支持写入Kafka、rabbitmq、nsq等消息队列
