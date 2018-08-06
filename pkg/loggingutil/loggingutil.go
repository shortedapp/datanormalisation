package loggingutil

import (
	"bytes"
	"flag"
	"log"
	"os"
	"sync"
	"time"
)

var once sync.Once
var Logger *LoggerImpl

type LoggerImpl struct {
	Level     int
	Vlogging  bool
	Logger    AppLogger
	StdLogger *log.Logger
}

type AppLogger interface {
	Debug(string, string)
	Info(string, string)
	Warn(string, string)
	Error(string, string)
}

type LogContext struct {
	App string
}

func init() {
	levelPtr := flag.Int("level", 5, "an int")
	vloggingPtr := flag.Bool("v", false, "a bool")
	flag.Parse()
	GetInstance(LogContext{App: "test"}, *levelPtr, *vloggingPtr)
}

func GetInstance(context LogContext, level int, vlogging bool) {
	once.Do(func() {
		Logger = createLoggerImpl(context, level, vlogging)
	})
}

func SetAppName(name string) {
	Logger.StdLogger.SetPrefix(time.Now().Format("2006/01/02 15:04:05.000") + " " + name)
}

func createLoggerImpl(l LogContext, level int, vlogging bool) *LoggerImpl {
	return &LoggerImpl{
		Level:     level,
		Vlogging:  vlogging,
		StdLogger: log.New(os.Stderr, l.App+":", 0),
	}
}

func Debug(function string, msg string) {
	if Logger.Vlogging {
		if Logger.Level <= 1 {
			Logger.StdLogger.Println(joinLogOutput(" [DEBUG] ", function, " ", msg))
		}
	}

}

func Info(function string, msg string) {
	if Logger.Vlogging {
		if Logger.Level <= 2 {
			Logger.StdLogger.Println(joinLogOutput(" [INFO] ", function, " ", msg))
		}
	}

}

func Warn(function string, msg string) {
	if Logger.Vlogging {
		if Logger.Level <= 3 {
			Logger.StdLogger.Println(joinLogOutput(" [WARN] ", function, " ", msg))
		}
	}

}

func Error(function string, msg string) {
	Logger.StdLogger.Println(joinLogOutput(" [ERROR] ", function, " ", msg))
}

func joinLogOutput(a ...string) string {
	var buf bytes.Buffer
	for _, str := range a {
		buf.WriteString(str)
	}
	return buf.String()
}
