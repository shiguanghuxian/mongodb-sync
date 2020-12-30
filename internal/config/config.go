package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/naoina/toml"
	"github.com/shiguanghuxian/mongodb-sync/internal/common"
)

// Config 配置文件
type Config struct {
	Debug bool          `toml:"debug" json:"debug,omitempty"`
	Mongo *MongoConfig  `toml:"mongo" json:"mongo,omitempty"`
	Sync  []*SyncConfig `toml:"sync" json:"sync,omitempty"`
}

type MongoConfig struct {
	SourceUri     string  `toml:"source_uri" json:"source_uri,omitempty"`
	SourceVersion float32 `toml:"source_version" json:"source_version,omitempty"`
}

func (c *MongoConfig) String() string {
	js, _ := json.Marshal(c)
	return string(js)
}

const (
	SyncTypeMongo = "mongo"
	SyncTypeEs    = "elasticsearch"
	SyncTypeMysql = "mysql"
	SyncTypeFile  = "file"
)

type SyncConfig struct {
	Enable          bool                `toml:"enable" json:"enable,omitempty"`                     // 是否启用
	Type            string              `toml:"type" json:"type,omitempty"`                         // mongo elasticsearch mysql file 一种输出类型只能配置一个，如果多个，请开启多个程序
	DestinationUri  string              `toml:"destination_uri" json:"destination_uri,omitempty"`   // 目标db链接地址
	SourceDb        string              `toml:"source_db" json:"source_db,omitempty"`               // 源db
	DestinationDb   string              `toml:"destination_db" json:"destination_db,omitempty"`     // 目标db
	Collections     map[string]string   `toml:"collections" json:"collections,omitempty"`           // 同步的集合对照 key:来源集合 val:目标集合或表等
	CollectionField map[string][]string `toml:"collection_field" json:"collection_field,omitempty"` // 需要同步的字段列表 - 下标为来源db的collection名值为同步的字段列表
}

func (cfg *SyncConfig) String() string {
	js, _ := json.Marshal(cfg)
	return string(js)
}

// 用于区分某个同步配置
func (cfg *SyncConfig) GetKey() string {
	if cfg == nil {
		return ""
	}
	str := fmt.Sprintf("%s-%s-%s-%s", cfg.Type, cfg.DestinationUri, cfg.SourceDb, cfg.DestinationDb)
	return str
}

func (cfg *SyncConfig) InCollectionField(collection, field string) bool {
	// 未配置，表示全部字段同步
	if len(cfg.CollectionField) == 0 {
		return true
	}
	for _, v := range cfg.CollectionField[collection] {
		if v == field {
			return true
		}
	}
	return false
}

// NewConfig 初始化一个server配置文件对象
func NewConfig(path string) (cfgChan chan *Config, err error) {
	if path == "" {
		path = common.GetRootDir() + "config/cfg.toml"
	}
	cfgChan = make(chan *Config, 0)
	// 读取配置文件
	cfg, err := readConfFile(path)
	if err != nil {
		return
	}
	go watcher(cfgChan, path)
	go func() {
		cfgChan <- cfg
	}()
	return
}

// ReadConfFile 读取配置文件
func readConfFile(path string) (cfg *Config, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	cfg = new(Config)
	if err := toml.NewDecoder(f).Decode(cfg); err != nil {
		return nil, err
	}
	if cfg.Mongo == nil || cfg.Mongo.SourceUri == "" {
		return nil, errors.New("mongo连接配置错误，请配置连接来源和目标连接地址")
	}
	// 同步库表配置不能为空
	if len(cfg.Sync) == 0 {
		return nil, errors.New("同步db配置不能为空")
	}
	for _, v := range cfg.Sync {
		if v.Type == "" || (v.DestinationUri == "" && v.Type != SyncTypeFile) || v.SourceDb == "" || v.DestinationDb == "" || len(v.Collections) == 0 {
			return nil, errors.New("同步collection配置错误")
		}
	}

	return
}
