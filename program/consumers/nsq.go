package consumers

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"go.mongodb.org/mongo-driver/bson"
)

/* 订阅mongo更新日志，写入kafka，方便其它程序订阅数据变化 */

type NsqConsumer struct {
	cfg      *config.SyncConfig
	producer *nsq.Producer
}

func NewNsqConsumer(cfg *config.SyncConfig) error {
	nsqConsumer := &NsqConsumer{
		cfg: cfg,
	}
	err := nsqConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), nsqConsumer)
	return nil
}

// 初始化连接
func (nc *NsqConsumer) InitClient(cfg *config.SyncConfig) (err error) {
	if cfg == nil || cfg.DestinationUri == "" {
		return errors.New("kafka链接配置错误")
	}
	nsqCfg := nsq.NewConfig()
	nsqCfg.LookupdPollInterval = 3 * time.Second
	nc.producer, err = nsq.NewProducer(cfg.DestinationUri, nsqCfg)
	if err != nil {
		return
	}
	go func() {
		for {
			err := nc.producer.Ping()
			if err != nil {
				logger.GlobalLogger.Errorw("ping nsq错误", "err", err)
			}
			time.Sleep(time.Minute)
		}
	}()
	return
}

// 销毁连接
func (nc *NsqConsumer) Disconnect() (err error) {
	// 注销
	unRegisterConsumer(nc.cfg.GetKey())
	// 关闭连接
	nc.producer.Stop()
	return
}

// 处理一条消息
func (nc *NsqConsumer) HandleData(data *models.ChangeEvent) (err error) {
	log.Println("kafka处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	collectionName := nc.cfg.Collections[data.Namespace.Coll]
	if collectionName == "" {
		collectionName = data.Namespace.Coll
	}
	// 整理发送消息
	msg := map[string]interface{}{
		"db":         data.Namespace.Db,
		"collection": collectionName,
		"operation":  data.Operation,
		"document":   data.Document,
	}
	val, _ := json.Marshal(msg)
	err = nc.producer.Publish(nc.cfg.DestinationDb, val)
	if err != nil {
		logger.GlobalLogger.Errorw("写入nsq消息错误", "err", err, "msg", msg)
		return
	}
	return
}

// 删除无用
func (nc *NsqConsumer) FilterField(collection string, document bson.M) (err error) {
	if document == nil {
		return
	}
	for k := range document {
		if !nc.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return
}
