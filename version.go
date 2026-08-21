// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "fmt"

var (
	Version          = "dev"
	GitCommit        = "unknown"
	BuildTime        = "unknown"
	SourceRepository = "https://github.com/apecloud/myduckserver"
)

func versionInfo() string {
	return fmt.Sprintf(
		"myduckserver version=%s commit=%s build_time=%s source=%s",
		Version,
		GitCommit,
		BuildTime,
		SourceRepository,
	)
}
