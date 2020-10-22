package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// mongodb 连接 来源和目标数据库连接

var (
	SourceClient *mongo.Client
)

// 源mongo链接
func InitSourceClient(cfg *config.Config) (err error) {
	if cfg.Mongo == nil || cfg.Mongo.SourceUri == "" {
		return fmt.Errorf("mongo配置错误: %v", cfg.Mongo)
	}
	SourceClient, err = InitClient(cfg.Mongo.SourceUri)
	return
}

// 初始化一个mongo链接
func InitClient(uri string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		logger.GlobalLogger.Errorw("连接源mongo错误", "err", err, "uri", uri)
		return nil, err
	}
	go func() {
		for {
			ping(client)
			time.Sleep(60 * time.Second)
		}
	}()
	return client, nil
}

// 定时ping mongo服务
func ping(client *mongo.Client) {
	if client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := client.Ping(ctx, readpref.Primary())
		if err != nil {
			logger.GlobalLogger.Errorw("ping 源mongo错误", "err", err)
		}
	}

}

// 注销源链接
func DisconnectSourceClient() {
	if SourceClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := SourceClient.Disconnect(ctx)
		if err != nil {
			logger.GlobalLogger.Errorw("关闭源mongo连接错误", "err", err)
		}
	}
}
