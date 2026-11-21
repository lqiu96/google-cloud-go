// Copyright 2015 Google LLC
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
	"fmt"
	"strings"

	bq "google.golang.org/api/bigquery/v2"
)

// An Error contains detailed information about a failed BigQuery operation
// returned in [JobStatus.Err], [JobStatus.Errors], or [RowInsertionError.Errors].
// Synchronous REST request failures return *[google.golang.org/api/googleapi.Error]
// instead, Storage Read API failures wrap gRPC status errors, and authentication
// failures may originate from golang.org/x/oauth2.
//
// Although Error implements the error interface with a value receiver, this
// package returns *Error values. Match them with errors.As using a pointer
// target (var bqErr *bigquery.Error).
//
// Inspect Reason (not only the HTTP status code) to distinguish transient
// failures (such as rateLimitExceeded, backendError, internalError,
// jobRateLimitExceeded, jobBackendError, and jobInternalError) from permanent
// errors. See https://docs.cloud.google.com/bigquery/docs/errors for API HTTP
// status and error reason codes, and
// https://docs.cloud.google.com/bigquery/docs/error-messages for client and
// connectivity errors.
type Error struct {
	// Mirrors bq.ErrorProto, but drops DebugInfo
	Location, Message, Reason string
}

func (e Error) Error() string {
	return fmt.Sprintf("{Location: %q; Message: %q; Reason: %q}", e.Location, e.Message, e.Reason)
}

func bqToError(ep *bq.ErrorProto) *Error {
	if ep == nil {
		return nil
	}
	return &Error{
		Location: ep.Location,
		Message:  ep.Message,
		Reason:   ep.Reason,
	}
}

// A MultiError contains multiple related errors, such as *[Error] entries inside
// [RowInsertionError.Errors]. MultiError does not implement Unwrap, so
// [errors.Is] and [errors.As] do not inspect elements inside it. Iterate over
// the slice directly.
type MultiError []error

func (m MultiError) Error() string {
	switch len(m) {
	case 0:
		return "(0 errors)"
	case 1:
		return m[0].Error()
	case 2:
		return m[0].Error() + " (and 1 other error)"
	}
	return fmt.Sprintf("%s (and %d other errors)", m[0].Error(), len(m)-1)
}

// RowInsertionError contains all errors that occurred when attempting to insert
// a row via [Inserter.Put]. RowInsertionError does not implement Unwrap.
// Inspect Errors directly for individual *[Error] reasons and messages.
type RowInsertionError struct {
	InsertID string // The InsertID associated with the affected row.
	RowIndex int    // The 0-based index of the affected row in the batch of rows being inserted.
	Errors   MultiError
}

func (e *RowInsertionError) Error() string {
	errFmt := "insertion of row [insertID: %q; insertIndex: %v] failed with error: %s"
	return fmt.Sprintf(errFmt, e.InsertID, e.RowIndex, e.Errors.Error())
}

// PutMultiError contains a [RowInsertionError] for each row rejected during
// [Inserter.Put]. Streaming inserts return HTTP 200 even when individual rows
// fail validation. PutMultiError does not implement Unwrap, so extract it with
// [errors.As] and iterate its [RowInsertionError] elements directly.
type PutMultiError []RowInsertionError

func (pme PutMultiError) errorDetails() string {
	size := len(pme)
	ellipsis := ""
	if size == 0 {
		return ""
	} else if size > 3 {
		size = 3
		ellipsis = ", ..."
	}

	es := make([]string, size)
	for i, e := range pme {
		if i >= size {
			break
		}
		es[i] = e.Error()
	}

	return fmt.Sprintf(" (%s%s)", strings.Join(es, ", "), ellipsis)
}

func (pme PutMultiError) Error() string {
	plural := "s"
	if len(pme) == 1 {
		plural = ""
	}

	details := pme.errorDetails()
	return fmt.Sprintf("%v row insertion%s failed%s", len(pme), plural, details)
}
