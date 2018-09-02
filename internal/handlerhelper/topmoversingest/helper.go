package topmoversingest

import (
	"github.com/shortedapp/shortedfunctions/internal/sharedata"
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
	log "github.com/shortedapp/shortedfunctions/pkg/loggingutil"
)

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutils.AwsUtiler
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {
	//Define result

	//Fetch latest data
	res := t.fetchLatestData()

	//Calculate movement
	for _, item := range res {
		t.calculateMovement(item)
	}

	//Order and number

	//Upload to dynamoDB

}

func (t *Topmoversingestor) fetchLatestData() []*sharedata.CombinedShortJSON {
	resp, err := t.Clients.FetchJSONFileFromS3("shortedappjmk", "combinedshorts.json", sharedata.UnmarshalCombinedShortsJSON)
	if err != nil {
		log.Info("IngestRoutine", "unable to fetch data from s3")
		return nil
	}
	return resp.([]*sharedata.CombinedShortJSON)
}

func (t *Topmoversingestor) calculateMovement(item *sharedata.CombinedShortJSON) sharedata.ShareMovementJSON {
	movement := sharedata.ShareMovementJSON{Code: item.Code}
	//TODO insert logic here
	return movement
}
