package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"go.mongodb.org/mongo-driver/bson"
)

/* 订阅mongo更新日志，写入kafka，方便其它程序订阅数据变化 */

type KafkaConsumer struct {
	cfg         *config.SyncConfig
	kafkaWriter *kafka.Writer
}

func NewKafkaConsumer(cfg *config.SyncConfig) error {
	kafkaConsumer := &KafkaConsumer{
		cfg: cfg,
	}
	err := kafkaConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), kafkaConsumer)
	return nil
}

// 初始化连接
func (kc *KafkaConsumer) InitClient(cfg *config.SyncConfig) (err error) {
	if cfg == nil || cfg.DestinationUri == "" {
		return errors.New("kafka链接配置错误")
	}
	brokers := strings.Split(cfg.DestinationUri, ";")
	kc.kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: brokers,
		Topic:   cfg.DestinationDb,
	})
	return
}

// 销毁连接
func (kc *KafkaConsumer) Disconnect() (err error) {
	// 注销
	unRegisterConsumer(kc.cfg.GetKey())
	// 关闭连接
	err = kc.kafkaWriter.Close()
	return
}

// 处理一条消息
func (kc *KafkaConsumer) HandleData(data *models.ChangeEvent) (err error) {
	log.Println("kafka处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	collectionName := kc.cfg.Collections[data.Namespace.Coll]
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
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	err = kc.kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(collectionName),
		Value: val,
	})
	if err != nil {
		logger.GlobalLogger.Errorw("写入kafka消息错误", "err", err, "msg", msg)
		return
	}
	return
}

// 删除无用
func (kc *KafkaConsumer) FilterField(collection string, document bson.M) (err error) {
	if document == nil {
		return
	}
	for k := range document {
		if !kc.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return
}
