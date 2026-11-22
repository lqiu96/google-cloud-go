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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery/internal"
	cloudinternal "cloud.google.com/go/internal"
	"cloud.google.com/go/internal/detect"
	"cloud.google.com/go/internal/trace"
	"cloud.google.com/go/internal/version"
	gax "github.com/googleapis/gax-go/v2"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
)

const (
	// Scope is the OAuth 2.0 scope for the service.
	// For relevant BigQuery scopes, see:
	// https://developers.google.com/identity/protocols/oauth2/scopes#bigquery
	Scope           = "https://www.googleapis.com/auth/bigquery"
	userAgentPrefix = "gcloud-golang-bigquery"
)

var xGoogHeader = fmt.Sprintf("gl-go/%s gccl/%s", version.Go(), internal.Version)

func setClientHeader(headers http.Header) {
	headers.Set("x-goog-api-client", xGoogHeader)
}

// Client may be used to perform BigQuery operations over the BigQuery REST/JSON
// API (and optionally the BigQuery Storage Read gRPC API when
// [Client.EnableStorageReadClient] is called).
//
// Clients should be reused across requests instead of created per request.
// After initial configuration (setting [Client.Location] and calling
// [Client.EnableStorageReadClient]), Client methods are safe for concurrent use
// by multiple goroutines. Iterators, [Job] status updates, and request builders
// ([Query], [Loader], [Extractor], [Copier], and [Inserter] option fields) are
// not safe for concurrent use. Call [Client.Close] when finished.
//
// Most RPCs automatically retry transient transport, HTTP 5xx (500, 502, 503,
// and 504), and selected structured BigQuery error reasons using hardcoded
// exponential backoff (gax.Backoff with Initial: 1s, Max: 32s, Multiplier: 2)
// until the context expires. Always pass a context with a deadline, and avoid
// wrapping RPCs in short per-attempt context timeouts inside an outer retry
// loop (which resets internal backoff to 1s on each attempt). Unstructured
// HTTP 429 responses, quotaExceeded errors, [ReaderSource] loads, and
// [Dataset.Create], [Routine.Create], [Model.Delete], and [Routine.Delete] are
// not retried automatically.
type Client struct {
	// Location, if set, is used as the default location for subsequent dataset
	// creation, job submission ([Query.Run], [Loader.Run], [Extractor.Run],
	// [Copier.Run]), and [Client.JobFromID] lookups. A location specified
	// directly on an operation ([JobIDConfig.Location],
	// [DatasetMetadata.Location], or [Client.JobFromIDLocation]) overrides this
	// value.
	//
	// Note that the [Query.Read] fast path (jobs.query) sends only
	// Query.Location ([JobIDConfig.Location]) and does not inherit
	// Client.Location. Set Query.Location directly when calling [Query.Read] in
	// a specific region, especially for queries or scripts that do not reference
	// a dataset.
	//
	// Set Location during client initialization before sharing the client across
	// goroutines. Single-region jobs outside the US and EU multi-regions require
	// a matching location, and multi-statement SQL scripts may require an
	// explicit location when initial statements do not reference a dataset.
	// Location must match the region of referenced datasets.
	// See https://docs.cloud.google.com/bigquery/docs/locations.
	Location string

	projectID string
	bqs       *bq.Service
	rc        *readClient

	// container for custom client options
	customConfig *customClientConfig
}

// DetectProjectID is a sentinel value that instructs [NewClient] to detect the
// project ID. It is given in place of the projectID argument. [NewClient] will
// use the project ID from the given credentials or the default credentials
// (https://docs.cloud.google.com/docs/authentication/application-default-credentials)
// if no credentials were provided. When providing credentials, not all
// options will allow [NewClient] to extract the project ID. Specifically a JWT
// does not have the project ID encoded.
//
// Pass DetectProjectID only to [NewClient], not to other methods such as
// [Client.DatasetInProject]. Call [Client.Project] to obtain the resolved
// project ID after client creation.
const DetectProjectID = "*detect-project-id*"

// NewClient constructs a new [Client] over the BigQuery REST/JSON API.
// Operations performed via the client are billed to the specified Google Cloud
// project. Clients should be reused instead of created per request. Call
// [Client.Close] when finished.
//
// The ctx argument is used only during client initialization (for credentials,
// transport setup, and project autodetection when [DetectProjectID] is passed).
// Subsequent RPC methods each use their own context argument. Credentials and
// client options are bound at client creation time.
func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	o := []option.ClientOption{
		option.WithScopes(Scope),
		option.WithUserAgent(fmt.Sprintf("%s/%s", userAgentPrefix, internal.Version)),
	}

	if gax.IsFeatureEnabled("TRACING") {
		o = append(o, internaloption.WithTelemetryAttributes(map[string]string{
			"gcp.client.repo":     "googleapis/google-cloud-go",
			"gcp.client.version":  internal.Version,
			"gcp.client.service":  "bigquery.googleapis.com",
			"gcp.client.artifact": "cloud.google.com/go/bigquery",
			"gcp.client.language": "go",
			"url.domain":          "bigquery.googleapis.com",
		}))
	}

	o = append(o, opts...)
	bqs, err := bq.NewService(ctx, o...)
	if err != nil {
		return nil, fmt.Errorf("bigquery: constructing client: %w", err)
	}

	// Handle project autodetection.
	projectID, err = detect.ProjectID(ctx, projectID, "", opts...)
	if err != nil {
		return nil, err
	}

	// gather any custom client options
	custom := newCustomClientConfig(opts...)

	c := &Client{
		projectID:    projectID,
		bqs:          bqs,
		customConfig: custom,
	}
	return c, nil
}

// EnableStorageReadClient opens a separate gRPC client for the BigQuery Storage
// Read API to accelerate large table, job, and query reads. Calling this method
// twice returns an error. Call it during client initialization before sharing
// the client across goroutines.
//
// Storage Read requires the bigquery.readsessions.create IAM permission (and
// bigquery.readsessions.getData to read streams). This method does not inherit
// [option.ClientOption] values (such as credentials, endpoints, or quota
// project) passed to [NewClient]. Pass them again here if needed.
//
// If read session setup fails (for example due to missing
// bigquery.readsessions.create permission or an unsupported source), reads
// silently fall back to the REST API. Note that [Table.Read] also fails on REST
// for views and external tables (query them with [Client.Query] instead).
// Single-page [Query.Read] fast-path results also stay on REST. Check
// [RowIterator.IsAccelerated] to confirm acceleration is active.
//
// When accelerated, [RowIterator.StartIndex] and [RowIterator.PageInfo]
// pagination are unsupported, [RowIterator.TotalRows] is an estimate from the
// read session, and callers should cancel the iterator's context if stopping
// before [google.golang.org/api/iterator.Done] to avoid leaking background
// reader goroutines.
func (c *Client) EnableStorageReadClient(ctx context.Context, opts ...option.ClientOption) error {
	if c.isStorageReadAvailable() {
		return fmt.Errorf("failed: storage read client already set up")
	}
	rc, err := newReadClient(ctx, c.projectID, opts...)
	if err != nil {
		return err
	}
	c.rc = rc
	return nil
}

func (c *Client) isStorageReadAvailable() bool {
	return c.rc != nil
}

// Project returns the project ID or number for this instance of the client, which may have
// either been explicitly specified or autodetected.
func (c *Client) Project() string {
	return c.projectID
}

// Close releases resources held by the client. Currently, Close only closes the
// BigQuery Storage Read gRPC client when [Client.EnableStorageReadClient] was
// called, and does not close idle HTTP connections on the underlying REST
// transport.
//
// Close should be called when the client is no longer needed (especially when
// Storage Read is enabled) to avoid leaking gRPC connections. Do not use the
// client after calling Close. It need not be called at program exit.
func (c *Client) Close() error {
	if c.isStorageReadAvailable() {
		err := c.rc.close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Calls the Jobs.Insert RPC and returns a Job.
func (c *Client) insertJob(ctx context.Context, job *bq.Job, media io.Reader, mediaOpts ...googleapi.MediaOption) (*Job, error) {
	ctx = setProjectItemTraceMetadata(ctx, c.projectID, "jobs")
	call := c.bqs.Jobs.Insert(c.projectID, job).Context(ctx)
	setClientHeader(call.Header())
	if media != nil {
		call.Media(media, mediaOpts...)
	}
	var res *bq.Job
	var err error
	invoke := func() error {
		sCtx := trace.StartSpan(ctx, "bigquery.jobs.insert")
		res, err = call.Do()
		trace.EndSpan(sCtx, err)
		return err
	}
	// A job with a client-generated ID can be retried; the presence of the
	// ID makes the insert operation idempotent.
	// We don't retry if there is media, because it is an io.Reader. We'd
	// have to read the contents and keep it in memory, and that could be expensive.
	// TODO(jba): Look into retrying if media != nil.
	if job.JobReference != nil && media == nil {
		// We deviate from default retries due to BigQuery wanting to retry structured internal job errors.
		err = runWithRetryExplicit(ctx, invoke, jobRetryReasons)
	} else {
		err = invoke()
	}
	if err != nil {
		return nil, err
	}
	return bqToJob(res, c)
}

// runQuery invokes the optimized query path.
// Due to differences in options it supports, it cannot be used for all existing
// jobs.insert requests that are query jobs.
func (c *Client) runQuery(ctx context.Context, queryRequest *bq.QueryRequest) (*bq.QueryResponse, error) {
	ctx = setProjectItemTraceMetadata(ctx, c.projectID, "queries")
	call := c.bqs.Jobs.Query(c.projectID, queryRequest).Context(ctx)
	setClientHeader(call.Header())

	var res *bq.QueryResponse
	var err error
	invoke := func() error {
		sCtx := trace.StartSpan(ctx, "bigquery.jobs.query")
		res, err = call.Do()
		trace.EndSpan(sCtx, err)
		return err
	}

	// We control request ID, so we can always runWithRetry.
	err = runWithRetryExplicit(ctx, invoke, jobRetryReasons)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Convert a number of milliseconds since the Unix epoch to a time.Time.
// Treat an input of zero specially: convert it to the zero time,
// rather than the start of the epoch.
func unixMillisToTime(m int64) time.Time {
	if m == 0 {
		return time.Time{}
	}
	return time.Unix(0, m*1e6)
}

// runWithRetry calls the function until it returns nil or a non-retryable error, or
// the context is done.
// See the similar function in ../storage/invoke.go. The main difference is the
// reason for retrying.
func runWithRetry(ctx context.Context, call func() error) error {
	return runWithRetryExplicit(ctx, call, defaultRetryReasons)
}

func runWithRetryExplicit(ctx context.Context, call func() error, allowedReasons []string) error {
	// These parameters match the suggestions in https://cloud.google.com/bigquery/sla.
	backoff := gax.Backoff{
		Initial:    1 * time.Second,
		Max:        32 * time.Second,
		Multiplier: 2,
	}
	return cloudinternal.Retry(ctx, backoff, func() (stop bool, err error) {
		err = call()
		if err == nil {
			return true, nil
		}
		return !retryableError(err, allowedReasons), err
	})
}

var (
	defaultRetryReasons = []string{"backendError", "rateLimitExceeded"}

	// These reasons are used exclusively for enqueuing jobs (jobs.insert and jobs.query).
	// Using them for polling may cause unwanted retries until context deadline/cancellation/etc.
	jobRetryReasons = []string{"backendError", "rateLimitExceeded", "jobRateLimitExceeded", "internalError"}

	retry5xxCodes = []int{
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	}
)

// retryableError is the unary retry predicate for this library.  In addition to structured error
// reasons, it specifies some HTTP codes (500, 502, 503, 504) and network/transport reasons.
func retryableError(err error, allowedReasons []string) bool {
	if err == nil {
		return false
	}
	if err == io.ErrUnexpectedEOF {
		return true
	}
	// Special case due to http2: https://github.com/googleapis/google-cloud-go/issues/1793
	// Due to Go's default being higher for streams-per-connection than is accepted by the
	// BQ backend, it's possible to get streams refused immediately after a connection is
	// started but before we receive SETTINGS frame from the backend.  This generally only
	// happens when we try to enqueue > 100 requests onto a newly initiated connection.
	if err.Error() == "http2: stream closed" {
		return true
	}
	if err.Error() == "http2: client connection lost" {
		return true
	}

	switch e := err.(type) {
	case *googleapi.Error:
		// We received a structured error from backend.
		var reason string
		if len(e.Errors) > 0 {
			reason = e.Errors[0].Reason
			if slices.Contains(allowedReasons, reason) {
				return true
			}
		}
		if slices.Contains(retry5xxCodes, e.Code) {
			return true
		}
	case *url.Error:
		retryable := []string{"connection refused", "connection reset"}
		for _, s := range retryable {
			if strings.Contains(e.Error(), s) {
				return true
			}
		}
	case interface{ Timeout() bool }:
		if e.Timeout() {
			return true
		}
	case interface{ Temporary() bool }:
		if e.Temporary() {
			return true
		}
	}
	// Check wrapped error.
	return retryableError(errors.Unwrap(err), allowedReasons)
}
