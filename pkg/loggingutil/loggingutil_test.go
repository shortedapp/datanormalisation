package loggingutil

import (
	"fmt"
	"strings"
	"testing"

	"github.com/shortedapp/datanormalization/pkg/testingutil"
	"github.com/stretchr/testify/assert"
)

func TestCreateInstance(t *testing.T) {
	var loggerCreateTests = []struct {
		input LoggerImpl // input
	}{
		{LoggerImpl{
			Level:    1,
			Vlogging: true,
		}},
		{LoggerImpl{
			Level:    2,
			Vlogging: true,
		}},
	}
	for _, testCondition := range loggerCreateTests {
		//Test default is set and unchangable
		CreateInstance(LogContext{"TEST"}, testCondition.input.Level, testCondition.input.Vlogging)
		assert.NotEqual(t, testCondition.input.Level, Logger.Level)
		assert.NotEqual(t, testCondition.input.Vlogging, Logger.Vlogging)
		assert.Equal(t, 5, Logger.Level)
		assert.Equal(t, false, Logger.Vlogging)
	}
}

func TestSetAppName(t *testing.T) {
	//set right level
	Logger.Level = 1
	Logger.Vlogging = true

	var loggerCreateTests = []string{
		"TestName1",
		"TestName2",
	}

	for _, testCondition := range loggerCreateTests {
		SetAppName(testCondition)
		log := testingutil.CaptureStandardErr(func() { Info("Test", "test") }, Logger.StdLogger)
		fmt.Println(log)
		assert.True(t, strings.Contains(log, testCondition))
	}
}

func TestInfo(t *testing.T) {

	var loggerCreateTests = []struct {
		level    int
		vlogging bool
		function string
		msg      string
		output   bool
	}{
		{1, true, "testA", "msgA", true},
		{3, true, "testB", "msgB", false},
		{1, false, "testC", "msgC", false},
	}

	for i, testCondition := range loggerCreateTests {
		Logger.Level = testCondition.level
		Logger.Vlogging = testCondition.vlogging
		log := testingutil.CaptureStandardErr(func() { Info(testCondition.function, testCondition.msg) }, Logger.StdLogger)
		fmt.Println("log: " + log)
		fmt.Println("msg: " + testCondition.msg)
		fmt.Println(strings.Contains(log, testCondition.msg))
		assert.Equal(t, testCondition.output, strings.Contains(log, testCondition.msg), fmt.Sprintf("test %v", i))
		assert.Equal(t, testCondition.output, strings.Contains(log, testCondition.function), fmt.Sprintf("test %v", i))
	}
}
