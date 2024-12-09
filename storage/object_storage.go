package storage

import "strings"

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
