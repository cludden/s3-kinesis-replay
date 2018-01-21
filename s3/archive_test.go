package s3

import (
	"errors"
	"s3-kinesis-replay/mock"
	"s3-kinesis-replay/replay"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	mocks "github.com/stretchr/testify/mock"
)

func TestNewArchiveInvalidConfig(t *testing.T) {
	testcases := []*struct {
		invalid bool
		config  *ArchiveConfig
	}{
		{true, NewArchiveConfig()},
		{true, &ArchiveConfig{
			Bucket:      "test",
			Client:      &mock.S3API{},
			Concurrency: 0,
			Downloader:  &mock.Downloader{},
			Log:         logrus.WithField("test", true),
		}},
		{false, &ArchiveConfig{
			Bucket:      "test",
			Client:      &mock.S3API{},
			Concurrency: 1,
			Downloader:  &mock.Downloader{},
			Log:         logrus.WithField("test", true),
			Prefix:      "foo",
			StartAfter:  "b",
			StopAt:      "d",
		}},
	}
	for _, testcase := range testcases {
		_, err := NewArchive(testcase.config)
		if testcase.invalid {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}
	}
}
func TestScanError(t *testing.T) {
	// create mock s3 client
	client := &mock.S3API{}
	client.On("ListObjectsV2Pages", mocks.AnythingOfType("*s3.ListObjectsV2Input"), mocks.AnythingOfType("")).Return(errors.New("unexpected"))

	// create archive using mock s3 client
	archive := &Archive{
		bucket: aws.String("foo"),
		client: client,
		log:    logrus.WithField("test", true),
	}

	// create scan arguments
	pending := make(chan *s3.Object, 2)

	// invoke scan
	archive.scan(pending)
	<-pending
}

func TestScanStopAt(t *testing.T) {
	// create mock s3 client
	output := &s3.ListObjectsV2Output{
		Contents: []*s3.Object{
			&s3.Object{
				Key: aws.String("a"),
			},
			&s3.Object{
				Key: aws.String("b"),
			},
			&s3.Object{
				Key: aws.String("c"),
			},
		},
		IsTruncated: aws.Bool(false),
		KeyCount:    aws.Int64(3),
	}
	client := &mock.S3API{}
	client.On("ListObjectsV2Pages", mocks.AnythingOfType("*s3.ListObjectsV2Input"), mocks.AnythingOfType("func(*s3.ListObjectsV2Output, bool) bool")).
		Run(func(args mocks.Arguments) {
			cb := args.Get(1).(func(*s3.ListObjectsV2Output, bool) bool)
			cb(output, true)
		}).
		Return(nil).
		Once()

	// create archive using mock s3 client
	archive := &Archive{
		bucket: aws.String("foo"),
		client: client,
		log:    logrus.WithField("test", true),
		stopAt: aws.String("c"),
	}

	// create scan arguments
	pending := make(chan *s3.Object, 2)

	// invoke scan
	archive.scan(pending)
	a := <-pending
	assert.Equal(t, "a", *a.Key)
	b := <-pending
	assert.Equal(t, "b", *b.Key)
	c, ok := <-pending
	assert.Nil(t, c)
	assert.Equal(t, false, ok)
}

func TestWorkerDownloadError(t *testing.T) {
	// create mock downloader
	downloader := &mock.Downloader{}
	downloader.On("Download", mocks.Anything, mocks.Anything).Return(int64(0), errors.New("unexpected")).Once()
	downloader.On("Download", mocks.Anything, mocks.Anything).Return(int64(1000), nil).Once()

	// create archive using mock downloader
	archive := &Archive{
		bucket:     aws.String("test"),
		downloader: downloader,
		log:        logrus.WithField("test", true),
	}
	// create worker arguments
	wg := &sync.WaitGroup{}
	pending := make(chan *s3.Object, 2)
	objects := make(chan *replay.Object, 2)
	// start two workers
	wg.Add(2)
	go archive.worker(wg, pending, objects)
	go archive.worker(wg, pending, objects)
	// commit an s3 object to the worker queue
	pending <- &s3.Object{
		Key: aws.String("foo"),
	}
	object := <-objects
	assert.Equal(t, "foo", *object.Object.Key)
	downloader.AssertExpectations(t)
}
