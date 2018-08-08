package sharecodes

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/shortedapp/datanormalization/pkg/csvutil"
)

type ShareJson struct {
	Name     string `json:"name"`
	Code     string `json:"code"`
	Industry string `json:"industry"`
}

type AsicShortJson struct {
	Name    string  `json:"name"`
	Code    string  `json:"code"`
	Shorts  int64   `json:"shorts"`
	Total   int64   `json:"total"`
	Percent float32 `json:"percent"`
}

type AsicShortCsv struct {
	Name    string
	Code    string
	Shorts  int64
	Total   int64
	Percent float32
}

type ShareCsv struct {
	Name     string
	Code     string
	Industry string
}

type CombinedShortJson struct {
	Code     string  `json:"code"`
	Name     string  `json:"name"`
	Shorts   int64   `json:"shorts"`
	Total    int64   `json:"total"`
	Percent  float32 `json:"percent"`
	Industry string  `json:"industry"`
}

type lengthError struct {
	len int
}

func (e *lengthError) Error() string {
	return fmt.Sprintf("len is too short: %i", e.len)
}

func (s *ShareCsv) Parse(str []string) error {
	if len(str) != 3 {
		return &lengthError{len(str)}
	}
	s.Name = str[0]
	s.Code = str[1]
	s.Industry = str[2]
	return nil
}

func (s *AsicShortCsv) Parse(str []string) error {
	if len(str) != 5 {
		return &lengthError{len(str)}
	}
	s.Name = str[0]
	s.Code = str[1]
	fmt.Println("string " + str[2])
	short, err := strconv.ParseInt(str[2], 0, 64)
	fmt.Println(err)
	if err != nil {
		//TODO return error
	}
	//TODO FIX THIS CONVERSION
	s.Shorts = 0 //int64(short)

	// total, err := strconv.ParseInt(str[4], 10, 64)
	fmt.Println(int64(short))
	if err != nil {
		//TODO return error
	}
	s.Total = 0 //total
	//TODO ADD A CONVERSION HERE
	s.Percent = 0.01
	return nil
}

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

func UnmarshalSharesJson(b []byte) (interface{}, error) {
	s1 := make([]*ShareJson, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}

func UnmarshalAsicShortsCSV(b []byte) ([]*AsicShortCsv, error) {
	s, err := csvutil.ReadCSVBytesNoChecks(b, '\t')
	if err != nil {
		fmt.Println(err)
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

func UnmarshalShortsJson(b []byte) (interface{}, error) {
	s1 := make([]*ShareJson, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}
