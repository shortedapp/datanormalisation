package csvutil

import (
	"bytes"
	"encoding/csv"
)

//ReadCSVBytesNoChecks - Read CSV byte array into a [][]string
func ReadCSVBytesNoChecks(b []byte, sep rune) ([][]string, error) {
	reader := csv.NewReader(bytes.NewReader(b))
	//Disable field length checks
	reader.FieldsPerRecord = -1
	reader.Comma = sep
	return reader.ReadAll()
}
