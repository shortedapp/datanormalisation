package moversdata

// swagger:model

//OrderedTopMovers
type OrderedTopMovers struct {
	Order       int     `json:"order"`
	DayCode     string  `json:"dayCode"`
	DayChange   float64 `json:"dayChange"`
	WeekCode    string  `json:"weekCode"`
	WeekChange  float64 `json:"weekChange"`
	MonthCode   string  `json:"monthCode"`
	MonthChange float64 `json:"monthChange"`
	YearCode    string  `json:"yearCode"`
	YearChange  float64 `json:"yearChange"`
}

//MoversByCode
type CodedTopMovers struct {
	Code        string  `json:"code"`
	DayChange   float64 `json:"dayChange"`
	WeekChange  float64 `json:"weekChange"`
	MonthChange float64 `json:"monthChange"`
	YearChange  float64 `json:"yearChange"`
}

type OrderedResultsJSON struct {
	Result []*OrderedTopMovers `json:"result"`
}

type CodedResultsJSON struct {
	Result []*CodedTopMovers `json:"result"`
}
