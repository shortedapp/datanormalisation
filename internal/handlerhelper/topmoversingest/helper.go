package topmoversingest

import (
	"github.com/shortedapp/shortedfunctions/pkg/awsutils"
)

//Topmoversingestor - struct to enable testing
type Topmoversingestor struct {
	Clients awsutils.AwsUtiler
}

//IngestMovement - Calaculate the movement and store in dynamoDB
func (t *Topmoversingestor) IngestMovement(tableName string) {

}
