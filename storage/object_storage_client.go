package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
)

// References:
// - S3 client:        https://docs.aws.amazon.com/code-library/latest/ug/go_2_s3_code_examples.html
// - Transfer Manager: https://aws.github.io/aws-sdk-go-v2/docs/sdk-utilities/s3/

type Bucket struct {
	S3Client *s3.Client
}

const (
	uploadPartSize   = 8 * 1024 * 1024 // 8 MiB
	downloadPartSize = 8 * 1024 * 1024 // 8 MiB
)

func NewBucket(config *aws.Config) *Bucket {
	return &Bucket{S3Client: s3.NewFromConfig(*config)}
}

func (basics *Bucket) UploadFile(ctx context.Context, bucketName string, objectKey string, fileName string) (*int64, error) {
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return nil, fmt.Errorf("Couldn't get file info for %v. Here's why: %v\n", fileName, err)
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("Couldn't open file %v to upload. Here's why: %v\n", fileName, err)
	} else {
		defer file.Close()

		uploader := manager.NewUploader(basics.S3Client, func(u *manager.Uploader) {
			u.PartSize = uploadPartSize
		})

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})

		if err != nil {
			var mu manager.MultiUploadFailure
			if errors.As(err, &mu) {
				return nil, fmt.Errorf("Error while uploading object to %s.\n"+
					"The UploadId is %s. Error is %v\n", bucketName, mu.UploadID(), err)
			} else {
				return nil, fmt.Errorf("Error while uploading object to %s.\n"+
					"Error is %v\n", bucketName, err)
			}
		}

		fileSize := fileInfo.Size()
		return &fileSize, nil
	}
}

func (basics *Bucket) DownloadFile(ctx context.Context, bucketName string, objectKey string, fileName string) (*int64, error) {
	f, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %q, %v", fileName, err)
	}
	defer f.Close()

	downloader := manager.NewDownloader(basics.S3Client, func(u *manager.Downloader) {
		u.PartSize = downloadPartSize
	})

	numBytes, err := downloader.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to download file, %v", err)
	}

	return &numBytes, nil
}
