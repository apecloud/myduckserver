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

import "testing"

func TestVersionInfo(t *testing.T) {
	originalVersion := Version
	originalGitCommit := GitCommit
	originalBuildTime := BuildTime
	originalSourceRepository := SourceRepository
	t.Cleanup(func() {
		Version = originalVersion
		GitCommit = originalGitCommit
		BuildTime = originalBuildTime
		SourceRepository = originalSourceRepository
	})

	Version = "v0.1.0-dev.20260821.1"
	GitCommit = "5e29d94db535e51876ec9465c5ef78a8e2c2d92a"
	BuildTime = "2026-08-21T15:35:20Z"
	SourceRepository = "https://github.com/apecloud/myduckserver"

	want := "myduckserver version=v0.1.0-dev.20260821.1 commit=5e29d94db535e51876ec9465c5ef78a8e2c2d92a build_time=2026-08-21T15:35:20Z source=https://github.com/apecloud/myduckserver"
	if got := versionInfo(); got != want {
		t.Fatalf("versionInfo() = %q, want %q", got, want)
	}
}
