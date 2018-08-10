package testingutil

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var logger *log.Logger

func TestCaptureStandardErr(t *testing.T) {
	logger := log.New(os.Stderr, "", log.LUTC)
	result := CaptureStandardErr(func() {
		logger.Println("test")
	}, logger)
	assert.EqualValues(t, "test\n", result)
}
