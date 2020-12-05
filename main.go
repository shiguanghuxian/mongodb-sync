package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/shiguanghuxian/mongodb-sync/internal/config"
	"github.com/shiguanghuxian/mongodb-sync/program"
)

var (
	P *program.Program
	// 版本信息
	VERSION    = "0.0.0"
	BUILD_TIME = ""
	GO_VERSION = ""
	GIT_HASH   = ""
)

func main() {
	// 系统日志显示文件和行号
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	// 输出版本
	if len(os.Args) > 1 {
		if os.Args[1] == "-v" || os.Args[1] == "version" {
			fmt.Printf("VERSION: %s\nBUILD_TIME: %s\nGO_VERSION: %s\nGIT_HASH: %s\n", VERSION, BUILD_TIME, GO_VERSION, GIT_HASH)
			return
		}
	}

	// 初始化配置文件
	cfgChan, err := config.NewConfig("")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	cfg := <-cfgChan
	// 程序实例
	P, err = program.New(cfg)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	// 运行
	log.Println("启动程序成功")
	P.Run()

	// 监听配置变化
	go restart(cfgChan)

	// 监听退出信号
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	P.Stop()
	log.Println("Exit")
}

// 重启应用
func restart(cfgChan chan *config.Config) {
	var err error
	for {
		cfg := <-cfgChan
		P.Stop()
		P, err = program.New(cfg)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
		}
		P.Run()
	}
}
