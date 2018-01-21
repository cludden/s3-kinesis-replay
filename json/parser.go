// Package json implements a parser for json serialized records
package json

import (
	"regexp"
	"s3-kinesis-replay/replay"
	"s3-kinesis-replay/validate"
	"sync"

	"github.com/Jeffail/gabs"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/sirupsen/logrus"
	"github.com/xeipuuv/gojsonschema"
)

// Parser implements a parser for json serialized records
type Parser struct {
	// The number of workers to spawn
	concurrency int
	// The delimiter to use for splitting record batches
	delimiter *regexp.Regexp
	// A logger instance
	log logrus.FieldLogger
	// The json path to the partition key field
	partitionKey string
	// An optional pattern to replace before splitting
	replace *regexp.Regexp
	// An optional string to use as a replacement
	replaceWith string
	// The reference path for the record's json schema
	schema *gojsonschema.Schema
	// A wait group to synchronise parser workers
	wg *sync.WaitGroup
}

// NewParser returns a new json parser
func NewParser(c *ParserConfig) (*Parser, error) {
	// validate config
	err := validate.V.Struct(c)
	if err != nil {
		return nil, err
	}
	// generate json schema

	// create new parser
	p := &Parser{
		concurrency:  c.Concurrency,
		log:          c.Log,
		partitionKey: c.PartitionKey,
		wg:           &sync.WaitGroup{},
	}
	// add json schema if included
	if c.Schema != "" {
		loader := gojsonschema.NewReferenceLoader(c.Schema)
		schema, err := gojsonschema.NewSchema(loader)
		if err != nil {
			return nil, err
		}
		p.schema = schema
	}
	// set optional settings
	if c.Delimiter != nil {
		p.delimiter = c.Delimiter
	}
	if c.Replace != nil && c.ReplaceWith != "" {
		p.replace = c.Replace
		p.replaceWith = c.ReplaceWith
	}
	return p, nil
}

// Parse spawns a pool of workers that process the incoming stream of objects, performing
// parsing and filtering logic before publishing to the entries stream. Parse blocks until
// all objects have been parsed and emitted.
func (p *Parser) Parse(objects chan *replay.Object, entries chan *kinesis.PutRecordsRequestEntry) {
	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go p.worker(p.wg, objects, entries)
	}
	p.wg.Wait()
	close(entries)
}

// worker creates a new worker that performs the actual parsing/filtering
// the configured delimiter, filtering invalid records using the defined jsons chema
func (p *Parser) worker(wg *sync.WaitGroup, objects chan *replay.Object, entries chan *kinesis.PutRecordsRequestEntry) {
	for o := range objects {
		log := p.log.WithField("key", *o.Object.Key)
		buff := o.Data

		// apply replacements
		if p.replace != nil {
			buff = p.replace.ReplaceAll(buff, []byte(p.replaceWith))
		}

		// build record set using delimiter if provided
		records := []string{}
		if p.delimiter != nil {
			records = p.delimiter.Split(string(buff), -1)
		} else {
			records = append(records, string(buff))
		}

		// parse the necessary parts of each record and filter out invalid records
		for _, raw := range records {
			// validate record against schema if defined
			if p.schema != nil {
				record := gojsonschema.NewStringLoader(raw)
				result, err := p.schema.Validate(record)
				if err != nil {
					log.WithError(err).Warnln("skipping record with validation error")
					continue
				} else if !result.Valid() {
					log.WithField("details", result.Errors()).Warnln("skipping invalid record")
					continue
				}
			}

			// parse record
			b := []byte(raw)
			parsed, err := gabs.ParseJSON(b)
			if err != nil {
				log.WithError(err).Warnln("unable to parse record")
				continue
			}

			// extract parition key using path
			partitionKey := parsed.Path(p.partitionKey).String()
			if partitionKey == "" {
				log.Warnln("missing parition key")
				continue
			}

			// build kinesis record and commit to entries stream
			entry := &kinesis.PutRecordsRequestEntry{
				PartitionKey: &partitionKey,
				Data:         b,
			}
			entries <- entry
		}
	}
	wg.Done()
}

// ParserConfig defines a json parser's configuration
type ParserConfig struct {
	Concurrency  int                `validate:"required,min=1"`
	Delimiter    *regexp.Regexp     `validate:"-"`
	Log          logrus.FieldLogger `validate:"required"`
	PartitionKey string             `validate:"required"`
	Replace      *regexp.Regexp     `validate:"-"`
	ReplaceWith  string             `validate:"-"`
	Schema       string             `validate:"-"`
}

// NewParserConfig returns a new config value with appropriate defaults
func NewParserConfig() *ParserConfig {
	return &ParserConfig{
		Concurrency: 1,
		Delimiter:   regexp.MustCompile("},{"),
		Log:         logrus.WithField("package", "json"),
		Replace:     regexp.MustCompile("}[\r\n]*{"),
		ReplaceWith: "}},{{",
	}
}
