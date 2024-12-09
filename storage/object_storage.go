package storage

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
	"path"
	"strings"
)

type ObjectStorageConfig struct {
	Provider        string
	Endpoint        string
	Region          string
	AccessKeyId     string
	SecretAccessKey string
}

var DefaultRegion = "cn-northwest-1"

func ParseS3RegionCode(endpoint string) string {
	if endpoint == "" {
		return ""
	}

	segments := strings.Split(endpoint, ".")
	last := len(segments) - 1
	if last < 0 {
		return ""
	}

	// If the last segment is 'cn', we adjust the index accordingly.
	if strings.EqualFold(segments[last], "cn") {
		last--
	}

	// Check that we have at least three segments before the last index
	if last >= 2 &&
		strings.EqualFold(segments[last], "com") &&
		strings.EqualFold(segments[last-1], "amazonaws") &&
		!strings.EqualFold(segments[last-2], "s3") {
		return segments[last-2]
	}

	return ""
}

func UploadLocalFile(storageConfig *ObjectStorageConfig, localDir, localFile, remotePath string) error {
	localFullPath := path.Join(localDir, localFile)
	file, err := os.Open(localFullPath)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", localFullPath, err)
	}
	defer file.Close()

	// Create an AWS config with static credentials
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(storageConfig.AccessKeyId, storageConfig.SecretAccessKey, "")),
		config.WithRegion(storageConfig.Region),
	)
	if err != nil {
		return fmt.Errorf("failed to load configuration, %v", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(awsCfg)

	// Parse the bucket and key from the remote path
	bucket, key := parseBucketAndPath(remotePath)
	if strings.HasSuffix(key, "/") {
		key += localFile
	}

	// Prepare the S3 put object input
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	}

	// Upload the file
	_, err = s3Client.PutObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}

	fmt.Printf("File %q successfully uploaded to s3://%s/%s\n", localFullPath, bucket, key)
	return nil
}

func parseBucketAndPath(fullPath string) (string, string) {
	parts := strings.SplitN(fullPath, "/", 2)
	if len(parts) < 2 {
		return fullPath, ""
	}
	return parts[0], parts[1]
}
