// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery

import (
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
)

// type for collecting custom ClientOption values.
type customClientConfig struct {
	jobCreationMode JobCreationMode
}

type customClientOption interface {
	option.ClientOption
	ApplyCustomClientOpt(*customClientConfig)
}

func newCustomClientConfig(opts ...option.ClientOption) *customClientConfig {
	conf := &customClientConfig{}
	for _, opt := range opts {
		if cOpt, ok := opt.(customClientOption); ok {
			cOpt.ApplyCustomClientOpt(conf)
		}
	}
	return conf
}

// JobCreationMode controls whether BigQuery creates a job when running a query
// on the [Query.Read] fast path (jobs.query). It has no effect on [Query.Run]
// or on queries that fall back to creating a job.
type JobCreationMode string

var (
	// JobCreationModeUnspecified is the default (unspecified) option.
	JobCreationModeUnspecified JobCreationMode = "JOB_CREATION_MODE_UNSPECIFIED"
	// JobCreationModeRequired indicates job creation is required.
	JobCreationModeRequired JobCreationMode = "JOB_CREATION_REQUIRED"
	// JobCreationModeOptional indicates job creation is optional, prioritizing
	// immediate results on the [Query.Read] fast path. When BigQuery executes a
	// query without creating a job, [RowIterator.SourceJob] returns nil and
	// [RowIterator.QueryID] identifies the query. The conditions under which
	// BigQuery avoids job creation are internal and subject to change.
	JobCreationModeOptional JobCreationMode = "JOB_CREATION_OPTIONAL"
)

// WithDefaultJobCreationMode is an [option.ClientOption] for [NewClient] that
// sets the default [JobCreationMode] for queries executed via the [Query.Read]
// fast path (jobs.query). It can be overridden per query via
// [QueryConfig.JobCreationMode] and has no effect on [Query.Run]. Users may
// experience lower latency with [JobCreationModeOptional].
func WithDefaultJobCreationMode(mode JobCreationMode) option.ClientOption {
	return &applierJobCreationMode{mode: mode}
}

// applier for propagating the custom client option to the config object
type applierJobCreationMode struct {
	internaloption.EmbeddableAdapter
	mode JobCreationMode
}

func (s *applierJobCreationMode) ApplyCustomClientOpt(c *customClientConfig) {
	c.jobCreationMode = s.mode
}
