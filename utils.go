package main

import (
	"crypto/rand"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"runtime"
	"strconv"
)

func setLogger(level string) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	log.SetLevel(lvl)

	if production {
		logsDir := "./logs"
		// ensure base logs directory exists
		if err := os.MkdirAll(logsDir, os.ModePerm); err != nil {
			log.Fatalf("Failed to create log directory: %v", err)
		}
		// choose role name (0=server, 1=client)
		roleName := "server"
		if role == 1 {
			roleName = "client"
		}
		// create a dedicated directory per node, e.g. ./logs/server0/
		dirPath := fmt.Sprintf("%s/%s%d", logsDir, roleName, myServerID)
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			log.Fatalf("Failed to create role log directory: %v", err)
		}
		// log file inside the node-specific directory, e.g. ./logs/server0/server0.log
		fileName := fmt.Sprintf("%s/%s%d.log", dirPath, roleName, myServerID)

		// Open with O_TRUNC so the file is replaced on each start. This gives
		// one log file per server/client.
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.Fatalf("Failed to create log file: %v", err)
		}

		log.SetOutput(file)
		log.SetReportCaller(true)
		log.Formatter = &logrus.TextFormatter{
			//ForceColors:               false,
			//DisableColors:             true,
			//ForceQuote:                false,
			//EnvironmentOverrideColors: false,
			//DisableTimestamp:          true,
			FullTimestamp:             true,
			TimestampFormat:           "2006-01-02 15:04:05",
			DisableSorting:            false,
			SortingFunc:               nil,
			DisableLevelTruncation:    false,
			PadLevelText:              false,
			QuoteEmptyFields:          false,
			FieldMap:                  nil,
			CallerPrettyfier:          nil,
		}
		
	} else {	
	log.SetReportCaller(true)
	log.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			fileName := path.Base(frame.File) + ":" + strconv.Itoa(frame.Line) + ""
			//return frame.Function, fileName
			return "", fileName + " >>"
		},
	FullTimestamp: true,
		})
		log.SetOutput(os.Stdout)
	}
}

func genRandomBytes(length int) (randomBytes []byte) {

	randomBytes = make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		fmt.Println("Error generating random bytes:", err)
		return
	}

	//fmt.Printf("Random Bytes: %x\n", randomBytes)
	return
}
