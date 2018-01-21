package main

import (
	"s3-kinesis-replay/cmd"

	"github.com/spf13/viper"
)

func main() {
	// define configuration file settings
	viper.SetConfigName("config")
	viper.AddConfigPath("/etc/s3-kinesis-replay")
	viper.AddConfigPath(".")

	// bind environment variables
	viper.BindEnv("json.concurrency", "JSON_CONCURRENCY")
	viper.BindEnv("json.partition_key", "JSON_PARTITION_KEY")
	viper.BindEnv("json.schema", "JSON_SCHEMA")
	viper.BindEnv("kinesis.backoff_interval", "KINESIS_BACKOFF_INTERVAL")
	viper.BindEnv("kinesis.backoff_max_interval", "KINESIS_MAX_BACKOFF_INTERVAL")
	viper.BindEnv("kinesis.buffer_size", "KINESIS_BUFFER_SIZE")
	viper.BindEnv("kinesis.buffer_window", "KINESIS_BUFFER_WINDOW")
	viper.BindEnv("kinesis.endpoint", "KINESIS_ENDPOINT")
	viper.BindEnv("kinesis.region", "KINESIS_REGION")
	viper.BindEnv("kinesis.stream_name", "KINESIS_STREAM_NAME")
	viper.BindEnv("log.format", "LOG_FORMAT")
	viper.BindEnv("log.level", "LOG_LEVEL")
	viper.BindEnv("parser.delimiter", "PARSER_DELIMITER")
	viper.BindEnv("parser.format", "PARSER_FORMAT")
	viper.BindEnv("parser.replace", "PARSER_REPLACE")
	viper.BindEnv("parser.replace_with", "PARSER_REPLACE_WITH")
	viper.BindEnv("s3.bucket", "S3_BUCKET")
	viper.BindEnv("s3.concurrency", "S3_CONCURRENCY")
	viper.BindEnv("s3.endpoint", "S3_ENDPOINT")
	viper.BindEnv("s3.prefix", "S3_PREFIX")
	viper.BindEnv("s3.region", "S3_REGION")
	viper.BindEnv("s3.start_after", "S3_START_AFTER")
	viper.BindEnv("s3.stop_at", "S3_STOP_AT")

	// set defaults
	viper.SetDefault("json.concurrency", 4)
	viper.SetDefault("json.delimiter", ",")
	viper.SetDefault("json.replace", "}[\r\n]*{")
	viper.SetDefault("json.replace_with", ",")
	viper.SetDefault("kinesis.backoff_interval", "1s")
	viper.SetDefault("kinesis.backoff_max_interval", "10s")
	viper.SetDefault("kinesis.buffer_window", "10s")
	viper.SetDefault("log.format", "json")
	viper.SetDefault("log.level", "info")
	viper.SetDefault("s3.concurrency", 4)

	// read config file
	err := viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			panic(err)
		}
	}

	// start the application
	cmd.Execute()
}
