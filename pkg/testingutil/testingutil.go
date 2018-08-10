package testingutil

import (
	"bytes"
	"log"
	"os"
)

//CaptureStandardErr - Capture the output from standard err (i.e logging) for testing
func CaptureStandardErr(f func(), logger *log.Logger) string {
	var buf bytes.Buffer
	defer func() {
		logger.SetOutput(os.Stderr)
	}()
	logger.SetOutput(&buf)
	f()

	return buf.String()
}
