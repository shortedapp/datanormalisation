package topshortslist

import (
	"fmt"

	"github.com/shortedapp/shortedfunctions/internal/searchutils"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

//Topshortslist - struct to enable testing
type Topshortslist struct {
	Clients awsutils.AwsUtiler
}

//FetchTopShorts -
func (t *Topshortslist) FetchTopShorts(period searchutils.SearchPeriod) {
	fmt.Println(searchutils.GetSearchWindow(t.Clients, "lastUpdate", "test", searchutils.Latest))
}
