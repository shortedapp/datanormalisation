package datafetch

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/shortedapp/shortedfunctions/pkg/awsutil"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Datafetch - struct to enable testing
type Datafetch struct {
	Clients awsutil.AwsUtiler
}

//FetchRoutine - Fetch a list of data on a daily basis
func (d *Datafetch) FetchRoutine(listOfFunctions ...interface{}) {
	for _, f := range listOfFunctions {
		//run the functions concurrently
		go f.(func())()
	}

}

//AsxCodeFetch - function to fetch asx codes
func (d *Datafetch) AsxCodeFetch() {
	resp, err := http.Get("https://www.asx.com.au/asx/research/ASXListedCompanies.csv")
	if err != nil {
		log.Info("AsxCodeFetch", "unable to fetch codes")
		return
	}
	//Marshall back to bytes
	result := filterLines(resp)

	//Put Results up to S3
	err = d.Clients.PutFileToS3("shortedappjmk", "ASXCodes.csv", result)
	if err == nil {
		log.Info("AsxCodeFetch", "completed put file to s3")
	}
}

func filterLines(resp *http.Response) []byte {
	//Result slice to append to
	var result []byte

	//Read the response body
	b, _ := ioutil.ReadAll(resp.Body)
	reader := bufio.NewReader(bytes.NewReader(b))

	//Loop to remove the unwanted rows of data
	i := 0
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if i > 2 {
			result = append(result, line...)
		}
		i++
	}

	return result
}
