package program

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/internal/logger"
	"github.com/shiguanghuxian/mongodb-sync/internal/models"
	"github.com/shiguanghuxian/mongodb-sync/internal/mongodb"
	"github.com/shiguanghuxian/mongodb-sync/program/consumers"
)

const (
	LastEventIdsFileName = "last_event_ids.json"
)

// Program 程序实体
type Program struct {
	cfg            *config.Config
	collectionChan map[string]chan *models.ChangeEvent
	lastEventIds   map[string][]byte // 保存最后watch的id
	mutex          sync.RWMutex
	stop           bool // 是否结束
}

// New 创建程序实例
func New(cfg *config.Config) (*Program, error) {
	// 初始化日志对象
	logger.NewLogger(cfg.Debug)
	// 初始化来源mongo链接
	err := mongodb.InitSourceClient(cfg)
	if err != nil {
		logger.GlobalLogger.Errorw("初始化源mongo连接错误", "err", err, "cfg", cfg)
	}
	// 初始化消费者
	for _, v := range cfg.Sync {
		if !v.Enable {
			continue
		}
		v := v
		var err error
		switch {
		case v.Type == config.SyncTypeMongo:
			err = consumers.NewMongoConsumer(v)
		case v.Type == config.SyncTypeFile:
			err = consumers.NewFileLogConsumer(v)
		case v.Type == config.SyncTypeEs:
			err = consumers.NewElasticsearchConsumer(v)
		case v.Type == config.SyncTypeMysql:
			err = consumers.NewMysqlConsumer(v, cfg.Debug)
		case v.Type == config.SyncTypeKafka:
			err = consumers.NewKafkaConsumer(v)
		case v.Type == config.SyncTypeRabbitmq:
			err = consumers.NewRabbitmqConsumer(v)
		case v.Type == config.SyncTypeNsq:
			err = consumers.NewNsqConsumer(v)
		default:
			logger.GlobalLogger.Warnw("不支持的的目标db类型", "type", v.Type)
			continue
		}
		if err != nil {
			log.Println("初始化目标消费者错误", v, err)
			logger.GlobalLogger.Errorw("初始化目标消费者错误", "err", err, "cfg", v)
		}
	}
	// 读取上次结束位置lastEventIds
	lastEventIds := make(map[string][]byte)
	body, err := ioutil.ReadFile(LastEventIdsFileName)
	if err != nil {
		logger.GlobalLogger.Warnw("读取上次结束位置lastEventIds错误", "err", err)
	} else {
		err = json.Unmarshal(body, &lastEventIds)
		if err != nil {
			logger.GlobalLogger.Warnw("解析上次结束位置lastEventIds错误", "err", err, "body", string(body))
		}
	}

	return &Program{
		cfg:          cfg,
		lastEventIds: lastEventIds,
		mutex:        sync.RWMutex{},
	}, nil
}

// Run 启动程序
func (p *Program) Run() {
	if p.cfg.Debug {
		js, _ := json.Marshal(p.cfg)
		log.Println(string(js))
	}
	// 判断mongo版本
	if p.cfg.Mongo.SourceVersion < 3.6 {
		logger.GlobalLogger.Errorw("不支持mongodb 3.6以下版本", "version", p.cfg.Mongo.SourceVersion)
		log.Println("不支持mongodb 3.6以下版本", p.cfg.Mongo.SourceVersion)
		os.Exit(1)
	}
	logger.GlobalLogger.Infow("启动mongodb-sync", "cfg", p.cfg)
	// 定时存储最后事件id
	go p.timerLastEventIds()
	// 运行同步逻辑
	p.sync()
}

// Stop 程序结束要做的事
func (p *Program) Stop() {
	p.stop = true // 标识结束
	logger.DestroyLogger()
	mongodb.DisconnectSourceClient()
	// 将最后处理id存储到文件
	js, _ := p.GetLastEventIdsToJsonBytes()
	err := ioutil.WriteFile(LastEventIdsFileName, js, os.ModePerm)
	if err != nil {
		logger.GlobalLogger.Infow("将lastEventIds写入文件错误", "err", err, "lastEventIds", string(js))
	}
	// 休息3秒，保证数据完成
	time.Sleep(3 * time.Second)
	// 销毁所有目标数据源
	for _, v := range p.cfg.Sync {
		if consumers.ConsumerMap[v.GetKey()] != nil {
			err := consumers.ConsumerMap[v.GetKey()].Disconnect()
			if err != nil {
				logger.GlobalLogger.Errorw("销毁目标db消费者错误", "err", err, "cfg", v)
			}
		}
	}
}

// 定时存储最后事件id到文件
// 防止中途未捕获的结束事件，导致丢失数据
func (p *Program) timerLastEventIds() {
	t := time.NewTicker(1 * time.Minute)
	for range t.C {
		log.Println("定时存储最后事件id到文件")
		if p.stop {
			return
		}
		js, _ := p.GetLastEventIdsToJsonBytes()
		err := ioutil.WriteFile(LastEventIdsFileName, js, os.ModePerm)
		if err != nil {
			logger.GlobalLogger.Infow("定时将lastEventIds写入文件错误", "err", err, "lastEventIds", string(js))
		}
	}
}
