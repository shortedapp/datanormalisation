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

//Logger - exported logger for access by other functions
var Logger *LoggerImpl

//LoggerImpl - Structure to store logger implementation details
type LoggerImpl struct {
	Level     int
	Vlogging  bool
	Logger    AppLogger
	StdLogger *log.Logger
}

//AppLogger - Interface for the various logger methods
type AppLogger interface {
	Debug(string, string)
	Info(string, string)
	Warn(string, string)
	Error(string, string)
}

//LogContext - Log Context structure to store the application name
type LogContext struct {
	App string
}

func init() {
	levelPtr := flag.Int("level", 5, "an int")
	vloggingPtr := flag.Bool("v", false, "a bool")
	flag.Parse()
	CreateInstance(LogContext{App: "test"}, *levelPtr, *vloggingPtr)
}

//CreateInstance - Creates a single instance of the logger for use around the application
func CreateInstance(context LogContext, level int, vlogging bool) {
	once.Do(func() {
		Logger = createLoggerImpl(context, level, vlogging)
	})
}

//SetAppName - Function to set the name of the application with time stamps for logging
func SetAppName(name string) {
	Logger.StdLogger.SetPrefix(time.Now().Format("2006/01/02 15:04:05.000") + " " + name)
}

//createLoggerImpl - Generate a logger implementation
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

//Info - Informational log output
func Info(function string, msg string) {
	if Logger.Vlogging {
		if Logger.Level <= 2 {
			Logger.StdLogger.Println(joinLogOutput(" [INFO] ", function, " ", msg))
		}
	}

}

//Warn - Warn log output
func Warn(function string, msg string) {
	if Logger.Vlogging {
		if Logger.Level <= 3 {
			Logger.StdLogger.Println(joinLogOutput(" [WARN] ", function, " ", msg))
		}
	}

}

//Error - error log output
func Error(function string, msg string) {
	Logger.StdLogger.Println(joinLogOutput(" [ERROR] ", function, " ", msg))
}

//joinLogOutput - Local function to join log strings
func joinLogOutput(a ...string) string {
	var buf bytes.Buffer
	for _, str := range a {
		buf.WriteString(str)
	}
	return buf.String()
}
