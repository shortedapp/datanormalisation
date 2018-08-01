package scheduledGet

import (
	"fmt"
	"net/http"
)

func ScheduledGetWithDynamoDB(url string) {
	resp, err := http.Head(url)

	if err != nil {
		return
	}

	lastModified := resp.Header.Get("Last-Modified")
	fmt.Println(lastModified)
}
