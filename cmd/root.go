package cmd

import (
	"errors"
	"regexp"
	"s3-kinesis-replay/json"
	"s3-kinesis-replay/kinesis"
	"s3-kinesis-replay/replay"
	"s3-kinesis-replay/s3"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	Kinesis "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	S3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// define application entrypoint
var rootCmd = &cobra.Command{
	Use:   "s3-kinesis-replay",
	Short: "a cli for replaying historical kinesis messages via an s3 archive",
	Args:  validateConfig,
	Run: func(cmd *cobra.Command, args []string) {
		// create application logger
		log := createLogger()

		// create aws session
		sess := session.Must(session.NewSession())

		// create s3 client and downloader
		var archive replay.Archive
		s3client := createS3Client(sess)
		s3downloader := createDownloader(s3client)
		archive = createArchive(log, s3client, s3downloader)

		// create kinesis client
		var producer replay.Producer
		kinesisClient := createKinesisClient(sess)
		producer = createProducer(log, kinesisClient)

		// create parser
		var parser replay.Parser
		format := viper.GetString("parser.format")
		if format == "json" {
			parser = createJSONParser(log)
		} else {
			log.Fatalln("invalid format")
		}

		// bootstrap application
		parser.Parse(archive.Scan(), producer.Stream())
		producer.Wait()
		log.Infoln("replay completed")
	},
}

// createArchive creates a new archive value
func createArchive(log logrus.FieldLogger, client s3iface.S3API, downloader s3.Downloader) replay.Archive {
	// create s3 archive
	archiveConfig := s3.NewArchiveConfig()
	archiveConfig.Bucket = viper.GetString("s3.bucket")
	archiveConfig.Concurrency = viper.GetInt("s3.concurrency")
	archiveConfig.Client = client
	archiveConfig.Downloader = downloader
	archiveConfig.Log = log.WithField("package", "s3")
	if viper.IsSet("s3.prefix") {
		archiveConfig.Prefix = viper.GetString("s3.prefix")
	}
	if viper.IsSet("s3.start_after") {
		archiveConfig.StartAfter = viper.GetString("s3.start_after")
	}
	if viper.IsSet("s3.start_after") {
		archiveConfig.StartAfter = viper.GetString("s3.start_after")
	}
	archive, err := s3.NewArchive(archiveConfig)
	if err != nil {
		log.WithError(err).Fatalln("error creating archive service")
	}
	return archive
}

// createDownloader returns a new s3 downloader
func createDownloader(client s3iface.S3API) s3.Downloader {
	return s3manager.NewDownloaderWithClient(client)
}

// createJSONParser returns a new json parser
func createJSONParser(log logrus.FieldLogger) replay.Parser {
	config := json.NewParserConfig()
	config.Log = log.WithField("package", "json")
	config.PartitionKey = viper.GetString("json.partition_key")
	config.Schema = viper.GetString("json.schema")
	if viper.IsSet("json.concurrency") {
		config.Concurrency = viper.GetInt("json.concurrency")
	}
	if delimiter := viper.GetString("parser.delimiter"); delimiter != "" {
		config.Delimiter = regexp.MustCompile(viper.GetString("parser.delimiter"))
	}
	if replace := viper.GetString("parser.replace"); replace != "" {
		replaceWith := viper.GetString("parser.replace_with")
		if replaceWith == "" {
			log.WithError(errors.New("if replace is defined, replace_with must also be defined")).
				Errorln("error creating json parser")
		}
		config.Replace = regexp.MustCompile(viper.GetString("parser.replace"))
		config.ReplaceWith = replaceWith
	}
	parser, err := json.NewParser(config)
	if err != nil {
		log.WithError(err).Fatalln("error creating json parser")
	}
	return parser
}

// createKinesisClient creates a new kinesis client
func createKinesisClient(sess *session.Session) kinesisiface.KinesisAPI {
	config := aws.NewConfig()
	if endpoint := viper.GetString("kinesis.endpoint"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}
	if region := viper.GetString("kinesis.region"); region != "" {
		config.Region = aws.String(region)
	}

	client := Kinesis.New(sess, config)
	return client
}

// createLogger configures and returns a new application scoped logger
func createLogger() logrus.FieldLogger {
	// set logging verbosity
	levels := map[string]logrus.Level{
		"debug": logrus.DebugLevel,
		"info":  logrus.InfoLevel,
		"warn":  logrus.WarnLevel,
		"error": logrus.ErrorLevel,
		"fatal": logrus.FatalLevel,
	}
	if level, ok := levels[viper.GetString("log.level")]; !ok {
		panic("invalid configuration: log.level")
	} else {
		logrus.SetLevel(level)
	}

	// set formatter
	format := viper.GetString("log.level")
	if format == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{})
	}

	// create scoped logger
	logger := logrus.WithField("name", "s3-kinesis-replay")
	return logger
}

// createProducer creates a new producer value
func createProducer(log logrus.FieldLogger, client kinesisiface.KinesisAPI) replay.Producer {
	config := kinesis.NewProducerConfig()
	config.Client = client
	config.Log = log.WithField("package", "kinesis")
	config.StreamName = viper.GetString("kinesis.stream_name")
	if backoffInterval := viper.GetDuration("kinesis.backoff_interval"); backoffInterval != time.Duration(0) {
		config.BackoffInterval = backoffInterval
	}
	if backoffMaxInterval := viper.GetDuration("kinesis.backoff_max_interval"); backoffMaxInterval != time.Duration(0) {
		config.BackoffMaxInterval = backoffMaxInterval
	}
	if bufferWindow := viper.GetDuration("kinesis.buffer_window"); bufferWindow != time.Duration(0) {
		config.BufferWindow = bufferWindow
	}
	producer, err := kinesis.NewProducer(config)
	if err != nil {
		log.WithError(err).Fatalln("error creating producer")
	}
	return producer
}

// createS3Client creates a new s3 client using the given session
func createS3Client(sess *session.Session) s3iface.S3API {
	config := aws.NewConfig()
	if endpoint := viper.GetString("s3.endpoint"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
	}
	if region := viper.GetString("s3.region"); region != "" {
		config.Region = aws.String(region)
	}
	client := S3.New(sess, config)
	return client
}

// Execute the root command
func Execute() {
	rootCmd.Execute()
}

// bind cli flags to application configuration
func init() {
	rootCmd.Flags().Int("json-concurrency", 4, "json parser concurrency")
	viper.BindPFlag("json.concurrency", rootCmd.Flags().Lookup("json-concurrency"))

	rootCmd.Flags().String("json-partition-key", "", "json parser parition key path")
	viper.BindPFlag("json.partition_key", rootCmd.Flags().Lookup("json-partition-key"))

	rootCmd.Flags().String("json-schema", "", "json parser schema path")
	viper.BindPFlag("json.schema", rootCmd.Flags().Lookup("json-schema"))

	rootCmd.Flags().String("kinesis-backoff-interval", "", "kinesis backoff interval")
	viper.BindPFlag("kinesis.backoff_interval", rootCmd.Flags().Lookup("kinesis-backoff-interval"))

	rootCmd.Flags().String("kinesis-backoff-max-interval", "", "kinesis max backoff interval")
	viper.BindPFlag("kinesis.backoff_max_interval", rootCmd.Flags().Lookup("kinesis-backoff-max-interval"))

	rootCmd.Flags().String("kinesis-buffer-window", "", "kinesis buffer window size")
	viper.BindPFlag("kinesis.buffer_window", rootCmd.Flags().Lookup("kinesis-buffer-window"))

	rootCmd.Flags().String("kinesis-endpoint", "", "kinesis endpoint override")
	viper.BindPFlag("kinesis.endpoint", rootCmd.Flags().Lookup("kinesis-endpoint"))

	rootCmd.Flags().String("kinesis-region", "", "kinesis region override")
	viper.BindPFlag("kinesis.region", rootCmd.Flags().Lookup("kinesis-region"))

	rootCmd.Flags().String("stream-name", "", "target kinesis stream name")
	viper.BindPFlag("kinesis.stream_name", rootCmd.Flags().Lookup("stream-name"))

	rootCmd.Flags().String("log-level", "", "log verbosity level")
	viper.BindPFlag("log.level", rootCmd.Flags().Lookup("log-level"))

	rootCmd.Flags().String("parser-format", "json", "parser format")
	viper.BindPFlag("parser.format", rootCmd.Flags().Lookup("parser-format"))

	rootCmd.Flags().String("parser-delimiter", "", "optional delimiter regexp")
	viper.BindPFlag("parser.delimiter", rootCmd.Flags().Lookup("parser-delimiter"))

	rootCmd.Flags().String("parser-replace", "", "optional replace regexp")
	viper.BindPFlag("parser.replace", rootCmd.Flags().Lookup("parser-replace"))

	rootCmd.Flags().String("parser-replace-with", "", "optional replacement string")
	viper.BindPFlag("parser.replace_with", rootCmd.Flags().Lookup("parser-replace-with"))

	rootCmd.Flags().String("bucket", "", "s3 archive bucket name")
	viper.BindPFlag("s3.bucket", rootCmd.Flags().Lookup("bucket"))

	rootCmd.Flags().String("prefix", "", "s3 archive prefix")
	viper.BindPFlag("s3.prefix", rootCmd.Flags().Lookup("prefix"))

	rootCmd.Flags().String("s3-region", "", "s3 archive region")
	viper.BindPFlag("s3.region", rootCmd.Flags().Lookup("s3-region"))

	rootCmd.Flags().String("start-after", "", "s3 archive start-after key")
	viper.BindPFlag("s3.start_after", rootCmd.Flags().Lookup("start-after"))

	rootCmd.Flags().String("stop-at", "", "s3 archive stop-at key")
	viper.BindPFlag("s3.stop_at", rootCmd.Flags().Lookup("stop-at"))
}

// validateConfig handles validating runtime configuration
func validateConfig(cmd *cobra.Command, args []string) error {
	validFormats := regexp.MustCompile("^(json)$")
	// validate parser format
	parserFormat := viper.GetString("parser.format")
	if !validFormats.MatchString(parserFormat) {
		return errors.New("invalid parser format")
	}
	// validate kinesis configuration
	if !viper.IsSet("kinesis.stream_name") {
		return errors.New("kinesis stream name is required")
	}
	// validate s3 configuration
	if !viper.IsSet("s3.bucket") {
		return errors.New("s3 bucket is required")
	}
	return nil
}
