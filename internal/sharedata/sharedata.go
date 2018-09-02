package sharedata

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/shortedapp/shortedfunctions/pkg/csvutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

// swagger:model

// ShareJSON - JSON structure for ASX share code information
type ShareJSON struct {
	Name     string `json:"name"`
	Code     string `json:"code"`
	Industry string `json:"industry"`
}

// AsicShortJSON - JSON structure for ASIC Shorted Stock information
type AsicShortJSON struct {
	Name    string  `json:"name"`
	Code    string  `json:"code"`
	Shorts  int64   `json:"shorts"`
	Total   int64   `json:"total"`
	Percent float32 `json:"percent"`
}

// TopShortJSON - JSON structure for Top Shorts
type TopShortJSON struct {
	Position int64   `json:"position"`
	Code     string  `json:"code"`
	Percent  float32 `json:"percent"`
}

// AsicShortCsv - CSV strucuture for ASIC Shorted Stock information
type AsicShortCsv struct {
	Name    string
	Code    string
	Shorts  int64
	Total   int64
	Percent float32
}

// ShareCsv - CSV structure for ASX Share Code information
type ShareCsv struct {
	Name     string
	Code     string
	Industry string
}

// CombinedShortJSON - JSON structure for combined ASIC short data and ASX code information
type CombinedShortJSON struct {
	Code     string  `json:"code"`
	Name     string  `json:"name"`
	Shorts   int64   `json:"shorts"`
	Total    int64   `json:"total"`
	Percent  float32 `json:"percent"`
	Industry string  `json:"industry"`
}

type ShareMovementJSON struct {
	Code  string  `json:"code"`
	Week  float64 `json:"week"`
	Month float64 `json:"month"`
	Year  float64 `json:"year"`
}

type lengthError struct {
	len int
}

func (e *lengthError) Error() string {
	return fmt.Sprintf("len is too short: %v", e.len)
}

//Parse - Parse CSV information into ShareCsv struct
func (s *ShareCsv) Parse(str []string) error {
	if len(str) != 3 {
		return &lengthError{len(str)}
	}
	s.Name = str[0]
	s.Code = str[1]
	s.Industry = str[2]
	return nil
}

//Parse - Parse CSV information into AsicShortCsv struct
func (s *AsicShortCsv) Parse(str []string) error {
	if len(str) != 5 {
		return &lengthError{len(str)}
	}

	s.Name = str[0]
	s.Code = strings.Trim(str[1], " ")
	short, err := strconv.ParseInt(str[2], 10, 64)
	if err != nil {
		log.Info("Parse-AsicShortCSV", "unable to convert short data to int64")
		return err
	}
	s.Shorts = short

	total, err := strconv.ParseInt(str[3], 10, 64)
	if err != nil {
		log.Info("Parse-AsicShortCSV", "unable to convert total share data to int64")
		return err
	}
	s.Total = total
	strconv.ParseFloat(str[4], 32)
	percent, err := strconv.ParseFloat(str[4], 32)
	if err != nil {
		log.Info("Parse-AsicShortCSV", "unable to convert percentage short data to float32")
		return err
	}
	s.Percent = float32(percent)
	return nil
}

//UnmarshalSharesCSV - Unmarshal Shares Csv information into a struct
func UnmarshalSharesCSV(s [][]string) (interface{}, error) {
	s1 := make([]*ShareCsv, 0, len(s))
	for _, str := range s {
		row := new(ShareCsv)
		err := row.Parse(str)
		if err == nil {
			s1 = append(s1, row)
		}
	}
	return s1, nil
}

//UnmarshalSharesJSON - Unmarshal Shares Json information into a structure
func UnmarshalSharesJSON(b []byte) (interface{}, error) {
	s1 := make([]*ShareJSON, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}

//UnmarshalSharesJSON - Unmarshal Shares Json information into a structure
func UnmarshalCombinedShortsJSON(b []byte) (interface{}, error) {
	s1 := make([]*CombinedShortJSON, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}

//UnmarshalAsicShortsCSV - Unmarshal Asic Shorts CSV information into a structure
func UnmarshalAsicShortsCSV(b []byte) ([]*AsicShortCsv, error) {
	s, err := csvutil.ReadCSVBytesNoChecks(b, '\t')
	if err != nil {
		log.Info("UnmarshalAsicShortsCSV", "unable to convert csv to strings")
	}
	s1 := make([]*AsicShortCsv, 0, len(s))
	for _, str := range s {
		row := new(AsicShortCsv)
		err := row.Parse(str)
		if err == nil {
			s1 = append(s1, row)
		}
	}
	return s1, nil
}

//UnmarshalShortsJSON - Unmarshal Asic Shorts JSON information into a structure
func UnmarshalShortsJSON(b []byte) (interface{}, error) {
	s1 := make([]*AsicShortJSON, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}
