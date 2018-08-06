package sharecodes

import (
	"encoding/json"
)

type Share struct {
	Name string `json:"name"`
	Code string `json:"code"`
}

func UnmarshalShares(b []byte) (interface{}, error) {
	s1 := make([]*Share, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}

type ShareCsv struct {
	Name string
	Code string
}

func (s *ShareCsv) Parse(str []string) {
	s.Name = str[0]
	s.Code = str[1]
}

func UnmarshalSharesCSV(s [][]string) (interface{}, error) {
	s1 := make([]*ShareCsv, 0, len(s))
	for _, str := range s {
		row := new(ShareCsv)
		row.Parse(str)
		s1 = append(s1, row)
	}
	return s1, nil
}
