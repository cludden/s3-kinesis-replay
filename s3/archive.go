// Package s3 provides types and methods for consuming an s3 message
// archive
package s3

import (
	"io"
	"s3-kinesis-replay/replay"
	"s3-kinesis-replay/validate"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
)

// Archive implements an s3 archive that utilizes an s3 download
// manager to download objects concurrently
type Archive struct {
	bucket      *string
	concurrency int
	client      s3iface.S3API
	downloader  Downloader
	log         logrus.FieldLogger
	prefix      *string
	startAfter  *string
	stopAt      *string
	wg          *sync.WaitGroup
}

// NewArchive returns a new s3 archive
func NewArchive(c *ArchiveConfig) (*Archive, error) {
	// validate configuration
	err := validate.V.Struct(c)
	if err != nil {
		return nil, err
	}
	// create new archiver
	a := &Archive{
		bucket:      aws.String(c.Bucket),
		client:      c.Client,
		concurrency: c.Concurrency,
		downloader:  c.Downloader,
		log:         c.Log,
		wg:          &sync.WaitGroup{},
	}
	// add optional parameters if valid
	if c.Prefix != "" {
		a.prefix = aws.String(c.Prefix)
	}
	if c.StartAfter != "" {
		a.startAfter = aws.String(c.StartAfter)
	}
	if c.StopAt != "" {
		a.stopAt = aws.String(c.StopAt)
	}
	return a, nil
}

// Scan implements logic for scanning some or all of an s3 message
// archive, downloading archive objects concurrently, and emitting
// them the returned channel
func (a *Archive) Scan() chan *replay.Object {
	// create buffered channel to queue s3 objects for downloading
	pending := make(chan *s3.Object, 1000)
	// create buffered channel to emit downloaded s3 objects
	objects := make(chan *replay.Object, 1000)
	// start download workers
	for i := 0; i < a.concurrency; i++ {
		a.wg.Add(1)
		go a.worker(a.wg, pending, objects)
	}
	// start archive scan
	go a.scan(pending)
	// add handler to close objects channel after all objects have
	// been downloaded
	go func() {
		a.wg.Wait()
		close(objects)
	}()
	return objects
}

// scan recursively scans an s3 bucket/prefix/startAfter and queues
// returned objects, stopping either when the specified
// stop at key is found or all objects have been downloaded
func (a *Archive) scan(pending chan *s3.Object) {
	// define list object parameters
	params := &s3.ListObjectsV2Input{
		Bucket:     a.bucket,
		Prefix:     a.prefix,
		StartAfter: a.startAfter,
	}
	// scan through object pages
	err := a.client.ListObjectsV2Pages(params, func(output *s3.ListObjectsV2Output, more bool) bool {
		for _, o := range output.Contents {
			if a.stopAt != nil && *a.stopAt == *o.Key {
				a.log.WithField("key", *o.Key).Infoln("stopping at stop key")
				return false
			}
			a.log.WithField("key", *o.Key).Debugln("queueing s3 key for download")
			pending <- o
		}
		return true
	})
	if err != nil {
		a.log.WithError(err).Errorln("scan:error")
	} else {
		a.log.Infoln("scan complete")
	}
	close(pending)
}

// worker manages downloading pending s3 objects
func (a *Archive) worker(wg *sync.WaitGroup, pending chan *s3.Object, objects chan *replay.Object) {
	for o := range pending {
		buff := &aws.WriteAtBuffer{}
		// download object
		n, err := a.downloader.Download(buff, &s3.GetObjectInput{
			Bucket: a.bucket,
			Key:    o.Key,
		})
		// on error, requeue object and kill worker
		if err != nil {
			a.log.WithError(err).Errorln("download error")
			pending <- o
			break
		}
		// log download info
		a.log.WithFields(logrus.Fields{
			"n":   n,
			"key": o.Key,
		}).Debugln("download complete")
		// emit processed object
		objects <- &replay.Object{
			Data:   buff.Bytes(),
			Object: o,
		}
	}
	wg.Done()
}

// ArchiveConfig defines an archive configuration
type ArchiveConfig struct {
	// The S3 bucket that contains the archive
	Bucket string `validate:"required"`
	// A configured s3 client
	Client s3iface.S3API `validate:"required"`
	// Number of objects to download in parallel
	Concurrency int `validate:"required,min=1"`
	// A configured s3 download manager
	Downloader Downloader `validate:"required"`
	// An optional archive scoped logger
	Log logrus.FieldLogger `valdiate:"required"`
	// An optional prefix that contains the relevant archive portion
	Prefix string `validate:"-"`
	// An optional s3 key to begin replay after
	StartAfter string `validate:"-"`
	// An optional s3 key to stop replay at
	StopAt string `validate:"-"`
}

// NewArchiveConfig returns an archive config value with appropriate
// defaults
func NewArchiveConfig() *ArchiveConfig {
	return &ArchiveConfig{
		Concurrency: 10,
		Log:         logrus.WithField("package", "s3"),
	}
}

// Downloader represents an s3 download manager
type Downloader interface {
	Download(io.WriterAt, *s3.GetObjectInput, ...func(*s3manager.Downloader)) (int64, error)
}
