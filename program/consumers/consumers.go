package consumers

import (
	"log"
	"time"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"go.mongodb.org/mongo-driver/bson"
)

/* 消费者，对于mongo数据变化处理插件 */

const (
	TimeoutCtx = 3 * time.Second
)

type Consumer interface {
	// 初始化连接
	InitClient(cfg *config.SyncConfig) error
	// 销毁连接
	Disconnect() error
	// 处理一条消息
	HandleData(data *models.ChangeEvent) error
	// 删除无用
	FilterField(collection string, document bson.M) error
}

var (
	ConsumerMap = make(map[string]Consumer) // 下标为一个sync配置
)

// 注册消费者
func registerConsumer(key string, consumer Consumer) {
	if consumer == nil {
		log.Println("消费者不能为nil")
		return
	}
	ConsumerMap[key] = consumer
}

// 删除一个消费者
func unRegisterConsumer(key string) {
	ConsumerMap[key] = nil
}

// 统一处理消息
func HandleData(key string, data *models.ChangeEvent) {
	for k, v := range ConsumerMap {
		if k == key {
			// 过滤字段
			err := v.FilterField(data.Namespace.Coll, data.Document)
			if err != nil {
				logger.GlobalLogger.Errorw("一个消费对象过滤字段出现错误", "err", err, "key", k, "namespace", data.Namespace)
			}
			// 交给对应消费者处理
			err = v.HandleData(data)
			if err != nil {
				logger.GlobalLogger.Errorw("一个消费对象处理出现错误", "err", err, "key", k, "namespace", data.Namespace)
			}
		}
	}
}
