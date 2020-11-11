# 目录介绍

此目录存储elasticsearch索引创建信息，用于不能自动创建索引的elasticsearch环境。

索引若存在，则不进行任何操作，需手动调整索引结构符合脚本配置同步字段列表。

## 规则

如下配置，本文件夹如果存在 `goods_index.json` 则会使用json文件信息创建索引

```
# 目标elasticsearch同步配置
[[sync]]
enable = true
type = "elasticsearch" # mongo elasticsearch mysql file 一种输出类型只能配置一个，如果多个，请开启多个程序
destination_uri = "http://127.0.0.1:9200" # 多个分号分割
source_db = "goods"
destination_db = "goods_index" # 对应 goods_index.json

# 同步的集合对照 key:来源集合 val:目标集合或表等
[sync.collections]
demo = "demo_bak"
```
