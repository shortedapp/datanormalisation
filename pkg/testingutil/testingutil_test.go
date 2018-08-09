package testingutil

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaptureStandardErr(t *testing.T) {
	logger := log.New(os.Stderr, "", log.LstdFlags)
	result := CaptureStandardErr(func() {
		logger.Println("test")
	})
	assert.EqualValues(t, "test\n", result)
}
