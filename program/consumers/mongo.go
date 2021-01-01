package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"github.com/shiguanghuxian/mongodb-sync/internal/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/* mogno目标数据落地 */
type MongoConsumer struct {
	client *mongo.Client
	cfg    *config.SyncConfig
}

// NewMongoConsumer 创建一个mongo消费对象
func NewMongoConsumer(cfg *config.SyncConfig) error {
	mongoConsumer := &MongoConsumer{
		cfg: cfg,
	}
	err := mongoConsumer.InitClient(cfg)
	if err != nil {
		return err
	}
	registerConsumer(cfg.GetKey(), mongoConsumer)
	return nil
}

// 初始化连接
func (mc *MongoConsumer) InitClient(cfg *config.SyncConfig) error {
	if cfg == nil || cfg.DestinationUri == "" {
		return errors.New("mongo目标db链接配置错误")
	}
	var err error
	mc.client, err = mongodb.InitClient(cfg.DestinationUri)
	if err != nil {
		return err
	}
	// 定时轮训防止连接断开
	go func() {
		for {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
				defer cancel()
				err := mc.client.Ping(ctx, nil)
				logger.GlobalLogger.Errorw("定时轮训防止连接断开错误", "err", err, "cfg", mc.cfg)
			}()
			time.Sleep(time.Minute)
		}
	}()

	return nil
}

// 销毁连接
func (mc *MongoConsumer) Disconnect() error {
	// 注销
	unRegisterConsumer(mc.cfg.GetKey())
	// 关闭连接
	if mc.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
		defer cancel()
		err := mc.client.Disconnect(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// 处理一条消息
func (mc *MongoConsumer) HandleData(data *models.ChangeEvent) (err error) {
	log.Println("mongo处理收到数据", data.Namespace.Db, data.Namespace.Coll, data.Operation)
	collectionName := mc.cfg.Collections[data.Namespace.Coll]
	if collectionName == "" {
		collectionName = data.Namespace.Coll
	}

	coll := mc.client.Database(mc.cfg.DestinationDb).Collection(collectionName)
	switch data.Operation {
	case "insert":
		err = mc.insert(coll, data)
	case "update":
		err = mc.update(coll, data)
	case "delete":
		err = mc.delete(coll, data)
	case "replace":
		err = mc.replace(coll, data)
	default:
		return errors.New("未知事件类型")
	}
	if err != nil {
		logger.GlobalLogger.Errorw("处理数据错误", "err", err, "data", data, "cfg", mc.cfg)
		return err
	}
	logger.GlobalLogger.Debugw("mongo数据处理成功", "data", data, "cfg", mc.cfg)
	return nil
}

// FilterField 删除无用
func (mc *MongoConsumer) FilterField(collection string, document bson.M) error {
	if document == nil {
		return nil
	}
	for k := range document {
		if !mc.cfg.InCollectionField(collection, k) {
			delete(document, k)
		}
	}
	return nil
}

// 插入数据
func (mc *MongoConsumer) insert(coll *mongo.Collection, data *models.ChangeEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	result, err := coll.InsertOne(ctx, data.Document)
	if err != nil {
		logger.GlobalLogger.Errorw("mongo将数据插入目标db错误", "err", err, "result", result, "data", data, "cfg", mc.cfg)
		return err
	}
	log.Println("插入数据成功", result.InsertedID)
	return nil
}

// 更新数据
func (mc *MongoConsumer) update(coll *mongo.Collection, data *models.ChangeEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	docCount, err := coll.CountDocuments(ctx, bson.M{"_id": data.DocumentKey.ID})
	if err != nil {
		logger.GlobalLogger.Errorw("更新前查询数据是否存在错误", "err", err, "data", data, "cfg", mc.cfg)
		return err
	}
	if docCount == 0 {
		return mc.insert(coll, data)
	}
	updateOpts := options.Update().SetUpsert(false)
	updateDoc := bson.M{"$set": data.Document}
	result, err := coll.UpdateOne(ctx, bson.M{"_id": data.DocumentKey.ID}, updateDoc, updateOpts)
	if err != nil {
		logger.GlobalLogger.Errorw("mongo将数据更新目标db错误", "err", err, "result", result, "data", data, "cfg", mc.cfg)
		return err
	}
	js, _ := json.Marshal(result)
	log.Println("更新数据成功", string(js))
	return nil
}

// 删除
func (mc *MongoConsumer) delete(coll *mongo.Collection, data *models.ChangeEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	result, err := coll.DeleteOne(ctx, bson.M{"_id": data.DocumentKey.ID})
	if err != nil {
		logger.GlobalLogger.Errorw("mongo删除目标db一条数据错误", "err", err, "result", result, "data", data, "cfg", mc.cfg)
		return err
	}
	log.Println("删除数据成功,删除数量", result.DeletedCount)
	return nil
}

// 查找结果，替换
func (mc *MongoConsumer) replace(coll *mongo.Collection, data *models.ChangeEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutCtx)
	defer cancel()
	result, err := coll.ReplaceOne(ctx, bson.M{"_id": data.DocumentKey.ID}, data.Document)
	if err != nil {
		logger.GlobalLogger.Errorw("mongo删除目标db一条数据错误", "err", err, "result", result, "data", data, "cfg", mc.cfg)
		return err
	}
	js, _ := json.Marshal(result)
	log.Println("replace更新数据成功", string(js))
	return nil
}
