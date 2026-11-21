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
	"reflect"

	"cloud.google.com/go/internal/trace"
	bq "google.golang.org/api/bigquery/v2"
)

// NoDedupeID indicates a streaming insert row wants to opt out of best-effort
// deduplication. Omitting insert IDs can increase streaming throughput, but
// automatic retries on transient errors may insert duplicate rows.
// It is EXPERIMENTAL and subject to change or removal without notice.
const NoDedupeID = "NoDedupeID"

// An Inserter does streaming inserts into a BigQuery table using the legacy
// streaming API (tabledata.insertAll). Streamed rows enter a temporary
// streaming buffer that can take up to 90 minutes to flush to managed storage,
// and are excluded from [TableMetadata.NumRows], [TableMetadata.NumBytes], and
// [RowIterator.TotalRows] until flushed. Deleting and recreating a table
// shortly before streaming can return 404 or drop rows for several minutes due
// to metadata eventual consistency.
//
// For new or high-throughput streaming workloads, prefer
// cloud.google.com/go/bigquery/storage/managedwriter.
//
// An Inserter is safe for concurrent calls to [Inserter.Put] only after its
// exported configuration fields have been set.
type Inserter struct {
	t *Table

	// SkipInvalidRows allows valid rows in a batch to be inserted even when
	// other rows fail validation. Invalid rows are still reported in the
	// returned [PutMultiError] and are not silently ignored. The default value
	// is false, which causes the entire batch to be rejected if any row is
	// invalid.
	SkipInvalidRows bool

	// IgnoreUnknownValues causes values not matching the schema to be ignored.
	// The default value is false, which causes records containing such values
	// to be treated as invalid records.
	IgnoreUnknownValues bool

	// A TableTemplateSuffix allows Inserters to create tables automatically.
	//
	// Experimental: this option is experimental and may be modified or removed in future versions,
	// regardless of any other documented package stability guarantees. In general,
	// the BigQuery team recommends the use of partitioned tables over sharding
	// tables by suffix.
	//
	// When you specify a suffix, the table you upload data to
	// will be used as a template for creating a new table, with the same schema,
	// called <table> + <suffix>.
	//
	// More information is available at
	// https://docs.cloud.google.com/bigquery/docs/write-api-rest#template-tables
	TableTemplateSuffix string
}

// Inserter returns an [Inserter] that can be used to append rows to t using the
// legacy streaming API (tabledata.insertAll). The returned Inserter may
// optionally be further configured before its [Inserter.Put] method is called.
// For new or high-throughput streaming workloads, prefer
// cloud.google.com/go/bigquery/storage/managedwriter.
//
// To stream rows into a specific partition of an ingestion-time partitioned
// table, add the $yyyymmdd partition decorator suffix to the table ID when
// constructing the [Table]. Partition decorators on column-partitioned tables
// require every row's partition column to match the target partition.
func (t *Table) Inserter() *Inserter {
	return &Inserter{t: t}
}

// Uploader calls Inserter.
// Deprecated: use Table.Inserter instead.
func (t *Table) Uploader() *Inserter { return t.Inserter() }

// Put uploads one or more rows to the BigQuery service.
//
// If src implements [ValueSaver], its Save method is called to produce a row
// for uploading. Pass *[StructSaver] (or []*[StructSaver]) rather than a value
// StructSaver, which returns an error.
//
// If src is a struct or pointer to a struct, a schema is inferred from its
// exported fields (see [InferSchema]) and used to create a [StructSaver].
// Unexported struct fields are silently skipped.
//
// If src is a slice of [ValueSaver] values, structs, or struct pointers, each
// element of the slice is treated as above and uploaded in a single batch. An
// empty slice is a no-op and returns nil without making a network request.
//
// When a row's InsertID is empty (including when passing structs directly), Put
// automatically generates a random insert ID per row before retrying to enable
// best-effort deduplication across retries. To opt out of deduplication for
// higher throughput, wrap rows in *[StructSaver] or implement [ValueSaver] with
// [NoDedupeID]. Note that retries may then insert duplicate rows.
//
// Streaming inserts return HTTP 200 even when individual rows fail validation.
// When one or more rows are rejected, Put returns a [PutMultiError] containing
// a [RowInsertionError] (with a 0-based RowIndex) for each failed row. Setting
// SkipInvalidRows allows valid sibling rows to succeed, while still returning
// [PutMultiError] for the rejected rows.
//
// Put retries transient request errors until ctx is canceled or expires, so
// always pass a context with a deadline. Row-level errors returned in
// [PutMultiError] are not retried automatically. See
// https://docs.cloud.google.com/bigquery/docs/errors for API HTTP status and
// error reason codes, and
// https://docs.cloud.google.com/bigquery/docs/error-messages for client and
// connectivity errors.
func (u *Inserter) Put(ctx context.Context, src any) (err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/bigquery.Inserter.Put")
	defer func() { trace.EndSpan(ctx, err) }()

	savers, err := valueSavers(src)
	if err != nil {
		return err
	}
	return u.putMulti(ctx, savers)
}

func valueSavers(src any) ([]ValueSaver, error) {
	saver, ok, err := toValueSaver(src)
	if err != nil {
		return nil, err
	}
	if ok {
		return []ValueSaver{saver}, nil
	}
	srcVal := reflect.ValueOf(src)
	if srcVal.Kind() != reflect.Slice {
		return nil, fmt.Errorf("%T is not a ValueSaver, struct, struct pointer, or slice", src)

	}
	var savers []ValueSaver
	for i := 0; i < srcVal.Len(); i++ {
		s := srcVal.Index(i).Interface()
		saver, ok, err := toValueSaver(s)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("src[%d] has type %T, which is not a ValueSaver, struct or struct pointer", i, s)
		}
		savers = append(savers, saver)
	}
	return savers, nil
}

// Make a ValueSaver from x, which must implement ValueSaver already
// or be a struct or pointer to struct.
func toValueSaver(x any) (ValueSaver, bool, error) {
	if _, ok := x.(StructSaver); ok {
		return nil, false, errors.New("bigquery: use &StructSaver, not StructSaver")
	}
	var insertID string
	// Handle StructSavers specially so we can infer the schema if necessary.
	if ss, ok := x.(*StructSaver); ok && ss.Schema == nil {
		x = ss.Struct
		insertID = ss.InsertID
		// Fall through so we can infer the schema.
	}
	if saver, ok := x.(ValueSaver); ok {
		return saver, ok, nil
	}
	v := reflect.ValueOf(x)
	// Support Put with []interface{}
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, false, nil
	}
	schema, err := inferSchemaReflectCached(v.Type())
	if err != nil {
		return nil, false, err
	}
	return &StructSaver{
		Struct:   x,
		InsertID: insertID,
		Schema:   schema,
	}, true, nil
}

func (u *Inserter) putMulti(ctx context.Context, src []ValueSaver) error {
	req, err := u.newInsertRequest(src)
	if err != nil {
		return err
	}
	if req == nil {
		return nil
	}
	ctx = setTableItemTraceMetadata(ctx, u.t.ProjectID, u.t.DatasetID, u.t.TableID, "insertAll")
	call := u.t.c.bqs.Tabledata.InsertAll(u.t.ProjectID, u.t.DatasetID, u.t.TableID, req).Context(ctx)
	setClientHeader(call.Header())
	var res *bq.TableDataInsertAllResponse
	err = runWithRetry(ctx, func() (err error) {
		sCtx := trace.StartSpan(ctx, "bigquery.tabledata.insertAll")
		res, err = call.Do()
		trace.EndSpan(sCtx, err)
		return err
	})
	if err != nil {
		return err
	}
	return handleInsertErrors(res.InsertErrors, req.Rows)
}

func (u *Inserter) newInsertRequest(savers []ValueSaver) (*bq.TableDataInsertAllRequest, error) {
	if savers == nil { // If there are no rows, do nothing.
		return nil, nil
	}
	req := &bq.TableDataInsertAllRequest{
		TemplateSuffix:      u.TableTemplateSuffix,
		IgnoreUnknownValues: u.IgnoreUnknownValues,
		SkipInvalidRows:     u.SkipInvalidRows,
	}
	for _, saver := range savers {
		row, insertID, err := saver.Save()
		if err != nil {
			return nil, err
		}
		if insertID == NoDedupeID {
			// User wants to opt-out of sending deduplication ID.
			insertID = ""
		} else if insertID == "" {
			insertID = randomIDFn()
		}
		m := make(map[string]bq.JsonValue)
		for k, v := range row {
			m[k] = bq.JsonValue(v)
		}
		req.Rows = append(req.Rows, &bq.TableDataInsertAllRequestRows{
			InsertId: insertID,
			Json:     m,
		})
	}
	return req, nil
}

func handleInsertErrors(ierrs []*bq.TableDataInsertAllResponseInsertErrors, rows []*bq.TableDataInsertAllRequestRows) error {
	if len(ierrs) == 0 {
		return nil
	}
	var errs PutMultiError
	for _, e := range ierrs {
		if int(e.Index) >= len(rows) {
			return fmt.Errorf("internal error: unexpected row index: %v", e.Index)
		}
		rie := RowInsertionError{
			InsertID: rows[e.Index].InsertId,
			RowIndex: int(e.Index),
		}
		for _, errp := range e.Errors {
			rie.Errors = append(rie.Errors, bqToError(errp))
		}
		errs = append(errs, rie)
	}
	return errs
}

// Uploader is an obsolete name for Inserter.
type Uploader = Inserter
