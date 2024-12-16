package storage

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"net/url"
	"path"
	"strings"
	"time"
)

type ObjectStorageConfig struct {
	Provider        string
	Endpoint        string
	Region          string
	AccessKeyId     string
	SecretAccessKey string
}

const (
	DummyRegion = "my-duck-server"
	HTTPPrefix  = "http://"
)

var supportedProvider = map[string]struct{}{
	"s3":  {},
	"s3c": {},
}

func (storageConfig *ObjectStorageConfig) UploadFile(localDir, localFile, remotePath string) (string, error) {
	startMillis := time.Now().UnixMilli()
	localFullPath := path.Join(localDir, localFile)
	s3Cfg, err := storageConfig.buildConfig()
	if err != nil {
		return "", err
	}

	bucket, key := parseBucketAndPath(remotePath)
	if strings.HasSuffix(key, "/") {
		key += localFile
	}

	backupBucket := NewBucket(s3Cfg)

	size, err := backupBucket.UploadFile(context.TODO(), bucket, key, localFullPath)
	if err != nil {
		return "", err
	}

	timeCost := time.Now().UnixMilli() - startMillis
	return fmt.Sprintf("Uploaded %s (%d bytes) to %s://%s/%s in %d ms\n",
		localFullPath, size, storageConfig.Provider, bucket, key, timeCost), nil
}

// DownloadFile downloads a file from the remote storage to the local storage.
func (storageConfig *ObjectStorageConfig) DownloadFile(remotePath, localDir, localFile string) (string, error) {
	startMillis := time.Now().UnixMilli()
	localFullPath := path.Join(localDir, localFile)
	s3Cfg, err := storageConfig.buildConfig()
	if err != nil {
		return "", err
	}

	// Parse the bucket and key from the remote path
	bucket, key := parseBucketAndPath(remotePath)
	if strings.HasSuffix(key, "/") {
		key += localFile
	}

	bucketBasics := NewBucket(s3Cfg)

	size, err := bucketBasics.DownloadFile(context.TODO(), bucket, key, localFullPath)
	if err != nil {
		return "", err
	}

	timeCost := time.Now().UnixMilli() - startMillis
	return fmt.Sprintf("Downloaded from %s://%s/%s (%d bytes) to %s in %d ms\n",
		storageConfig.Provider, bucket, key, size, localFullPath, timeCost), nil

}

func (storageConfig *ObjectStorageConfig) buildConfig() (cfg *aws.Config, err error) {
	var s3Cfg aws.Config
	if storageConfig.Provider == "s3c" {
		s3Cfg, err = storageConfig.buildConfigForS3Compatible()
		if err != nil {
			return nil, fmt.Errorf("failed to build config for s3c, %v", err)
		}
	} else if storageConfig.Provider == "s3" {
		s3Cfg, err = storageConfig.buildConfigForS3()
		if err != nil {
			return nil, fmt.Errorf("failed to build config for s3, %v", err)
		}
	} else {
		return nil, fmt.Errorf("unsupported provider %q", storageConfig.Provider)
	}
	return &s3Cfg, nil
}

func (storageConfig *ObjectStorageConfig) buildConfigForS3Compatible() (cfg aws.Config, err error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               HTTPPrefix + storageConfig.Endpoint,
			SigningRegion:     storageConfig.Region,
			HostnameImmutable: true,
		}, nil
	})

	return awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(storageConfig.AccessKeyId, storageConfig.SecretAccessKey, "")),
		awsconfig.WithRegion(storageConfig.Region),
		awsconfig.WithEndpointResolverWithOptions(customResolver))
}

func (storageConfig *ObjectStorageConfig) buildConfigForS3() (cfg aws.Config, err error) {
	return awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(storageConfig.AccessKeyId, storageConfig.SecretAccessKey, "")),
		awsconfig.WithRegion(storageConfig.Region),
	)
}

func ConstructStorageConfig(remoteUri, endpoint, accessKeyId, secretAccessKey string) (*ObjectStorageConfig, string, error) {
	provider, remotePath, err := parseProviderAndPath(remoteUri)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse remote path: %w", err)
	}

	region, err := parseRegionFromEndpoint(provider, endpoint)
	if err != nil {
		return nil, "", fmt.Errorf("failed to parse region from endpoint: %w", err)
	}

	storageConfig := &ObjectStorageConfig{
		Provider:        provider,
		Endpoint:        endpoint,
		AccessKeyId:     accessKeyId,
		SecretAccessKey: secretAccessKey,
		Region:          region,
	}

	return storageConfig, remotePath, nil
}

func parseBucketAndPath(fullPath string) (string, string) {
	parts := strings.SplitN(fullPath, "/", 2)
	if len(parts) < 2 {
		return fullPath, ""
	}
	return parts[0], parts[1]
}

func parseProviderAndPath(uri string) (string, string, error) {
	parsedUri, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse remote path: %w", err)
	}

	provider := strings.ToLower(parsedUri.Scheme)
	if _, ok := supportedProvider[provider]; !ok {
		return "", "", fmt.Errorf("unsopported Provider, please use s3 or s3c: %w", err)
	}

	return provider, parsedUri.Host + parsedUri.Path, nil
}

func parseRegionFromEndpoint(provider, endpoint string) (string, error) {
	region := ""
	if provider == "s3" {
		region = parseS3RegionCode(endpoint)
		if region == "" {
			return "", fmt.Errorf("missing region in endpoint: %s", endpoint)
		}
	} else {
		region = DummyRegion
	}
	return region, nil
}

func parseS3RegionCode(endpoint string) string {
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
