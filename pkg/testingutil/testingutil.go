package testingutil

import (
	"bytes"
	"log"
	"os"
)

//CaptureStandardErr - Capture the output from standard err (i.e logging) for testing
func CaptureStandardErr(f func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	f()
	defer func() {
		log.SetOutput(os.Stderr)
	}()
	return buf.String()
}
