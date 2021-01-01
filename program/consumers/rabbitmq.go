package consumers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
)

/* 订阅mongo更新日志，写入kafka，方便其它程序订阅数据变化 */

type RabbitmqConsumer struct {
	cfg  *config.SyncConfig
	conn *amqp.Connection
}

func NewRabbitmqConsumer(cfg *config.SyncConfig) error {
	rabbitmqConsumer := &RabbitmqConsumer{
		cfg: cfg,
	}
	err := rabbitmqConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), rabbitmqConsumer)
	return nil
}

// 初始化连接
func (rc *RabbitmqConsumer) InitClient(cfg *config.SyncConfig) (err error) {
	if cfg == nil || cfg.DestinationUri == "" {
		return errors.New("kafka链接配置错误")
	}
	rc.conn, err = amqp.Dial(cfg.DestinationUri)
	return
}

// 销毁连接
func (rc *RabbitmqConsumer) Disconnect() (err error) {
	// 注销
	unRegisterConsumer(rc.cfg.GetKey())
	// 关闭连接
	err = rc.conn.Close()
	return
}

// 处理一条消息
func (rc *RabbitmqConsumer) HandleData(data *models.ChangeEvent) (err error) {
	log.Println("rabbitmq处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	collectionName := rc.cfg.Collections[data.Namespace.Coll]
	if collectionName == "" {
		collectionName = data.Namespace.Coll
	}
	defer func() {
		if err != nil {
			logger.GlobalLogger.Errorw("写入rabbitmq消息错误", "err", err)
		}
	}()
	// 整理发送消息
	ch, err := rc.conn.Channel()
	if err != nil {
		return
	}
	defer ch.Close()
	err = ch.ExchangeDeclarePassive(
		collectionName,
		amqp.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return
	}
	routingKey := fmt.Sprintf("mongodb-sync.%s", data.Operation)
	msg := map[string]interface{}{
		"db":         data.Namespace.Db,
		"collection": collectionName,
		"operation":  data.Operation,
		"document":   data.Document,
	}
	val, _ := json.Marshal(msg)
	err = ch.Publish(
		collectionName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "utf-8",
			Body:            val,
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
		},
	)
	return
}

// 删除无用
func (rc *RabbitmqConsumer) FilterField(collection string, document bson.M) (err error) {
	if document == nil {
		return
	}
	for k := range document {
		if !rc.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return
}
