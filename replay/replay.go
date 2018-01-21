// Package replay implements types and interfaces that describe the appication
// domain
package replay

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Archive is a consumer responsible for scanning an s3 message archive
// and returning a buffered stream of s3 objects
type Archive interface {
	// Scan the s3 archive and emit objects on the returned channel
	Scan() chan *Object
}

// Object is a wrapper around an s3 object that includes the downloaded
// object data
type Object struct {
	Data   []byte
	Object *s3.Object
}

// Parser is responsible for processing the stream of archived s3 messages
// and preparing them for replay
type Parser interface {
	Parse(chan *Object, chan *kinesis.PutRecordsRequestEntry)
}

// Producer is responsible for replaying processed messages to kinesis
type Producer interface {
	// Returns a channel that accepts kinesis messages to replay
	Stream() chan *kinesis.PutRecordsRequestEntry
	// Wait for producer to finish replaying messages
	Wait()
}
