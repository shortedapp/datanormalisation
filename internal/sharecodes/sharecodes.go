package sharecodes

import "encoding/json"

type Share struct {
	Name string `json:"name"`
	Code string `json:"code"`
}

func UnmarshalShares(b []byte) (interface{}, error) {
	s1 := make([]*Share, 0)
	err := json.Unmarshal(b, &s1)
	return s1, err
}
