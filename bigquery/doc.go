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

/*
Package bigquery provides a client for the BigQuery service over the BigQuery
REST/JSON API, with optional acceleration over the BigQuery Storage Read gRPC
API via [Client.EnableStorageReadClient].

The following assumes a basic familiarity with BigQuery concepts.
See https://docs.cloud.google.com/bigquery/docs.

See https://pkg.go.dev/cloud.google.com/go for authentication, timeouts,
connection pooling, and similar aspects of this package.

# Choosing a BigQuery Package

This module includes several packages serving different use cases:

  - cloud.google.com/go/bigquery (this package, GA): primary client for
    queries, jobs, datasets, tables, models, routines, bulk loads, extracts,
    copies, and legacy streaming inserts ([Inserter]).
  - cloud.google.com/go/bigquery/storage/managedwriter (GA,
    https://pkg.go.dev/cloud.google.com/go/bigquery/storage/managedwriter):
    recommended client for new or high-throughput streaming ingestion using
    the BigQuery Storage Write API.
  - cloud.google.com/go/bigquery/storage/apiv1 (GA,
    https://pkg.go.dev/cloud.google.com/go/bigquery/storage/apiv1): low-level
    gRPC GAPIC clients for direct Storage Read and Write stream control.
  - cloud.google.com/go/bigquery/v2 (experimental): experimental next-generation
    GAPIC clients. Not a replacement for this package.
  - Companion control-plane GAPIC subpackages (connection/apiv1,
    datatransfer/apiv1, reservation/apiv1, biglake/apiv1, analyticshub/apiv1,
    datapolicies/apiv1, and migration/apiv2): separate administrative services.

# Creating a Client

To start working with this package, create a client with [NewClient]. Reuse a
single client across requests instead of creating one per request, and call
[Client.Close] when finished:

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
	    // TODO: Handle error.
	}
	defer client.Close()

After initial configuration (setting [Client.Location] and calling
[Client.EnableStorageReadClient]), [Client] methods and resource handles
([Dataset], [Table], [Model], [Routine]) are safe for concurrent use by
multiple goroutines. Iterators ([RowIterator], [JobIterator], and others),
[Job] status updates, and request builders ([Query], [Loader], [Extractor],
[Copier], and [Inserter] option fields) are not safe for concurrent use.

# Querying

To query existing tables, create a [Client.Query] and call its [Query.Read] method, which starts the
query and waits for it to complete:

	q := client.Query(`
	    SELECT year, SUM(number) as num
	    FROM bigquery-public-data.usa_names.usa_1910_2013
	    WHERE name = @name
	    GROUP BY year
	    ORDER BY year
	`)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "name", Value: "William"},
	}
	it, err := q.Read(ctx)
	if err != nil {
	    // TODO: Handle error.
	}

Then iterate through the resulting rows. You can store a row using
anything that implements the [ValueLoader] interface, or with a slice or map of [Value].
A slice is simplest:

	for {
	    var values []bigquery.Value
	    err := it.Next(&values)
	    if err == iterator.Done {
	        break
	    }
	    if err != nil {
	        // TODO: Handle error.
	    }
	    fmt.Println(values)
	}

You can also use a struct whose exported fields match the query:

	type Count struct {
	    Year int
	    Num  int
	}
	for {
	    var c Count
	    err := it.Next(&c)
	    if err == iterator.Done {
	        break
	    }
	    if err != nil {
	        // TODO: Handle error.
	    }
	    fmt.Println(c)
	}

You can also start the query running and get the results later.
Create the query as above, but call [Query.Run] instead of [Query.Read]. This returns a [Job],
which represents an asynchronous operation.

	job, err := q.Run(ctx)
	if err != nil {
	    // TODO: Handle error.
	}

Get the job's ID, a printable string. You can save this string to retrieve
the results at a later time, even in another process.

	jobID := job.ID()
	fmt.Printf("The job ID is %s\n", jobID)

To retrieve the job's results from the ID, first look up the [Job] with the [Client.JobFromID] method:

	job, err = client.JobFromID(ctx, jobID)
	if err != nil {
	    // TODO: Handle error.
	}

Use the [Job.Read] method to obtain an iterator, and loop over the rows.
Calling [Query.Read] is preferred for queries with a relatively small result set,
as it will call BigQuery jobs.query API for an optimized query path. If the query
doesn't meet that criteria, the method will just combine [Query.Run] and [Job.Read].

	it, err = job.Read(ctx)
	if err != nil {
	    // TODO: Handle error.
	}
	// Proceed with iteration as above.

# Datasets and Tables

You can refer to datasets in the client's project with the [Client.Dataset] method, and
in other projects with the [Client.DatasetInProject] method:

	myDataset := client.Dataset("my_dataset")
	yourDataset := client.DatasetInProject("your-project-id", "your_dataset")

These methods create references to datasets, not the datasets themselves. You can have
a dataset reference even if the dataset doesn't exist yet. Use [Dataset.Create] to
create a dataset from a reference:

	if err := myDataset.Create(ctx, nil); err != nil {
	    // TODO: Handle error.
	}

You can refer to tables with [Dataset.Table]. Like [Dataset], [Table] is a reference
to an object in BigQuery that may or may not exist.

	table := myDataset.Table("my_table")

You can create, delete and update the metadata of tables with methods on [Table].
For instance, you could create a temporary table with:

	err = myDataset.Table("temp").Create(ctx, &bigquery.TableMetadata{
	    ExpirationTime: time.Now().Add(1*time.Hour)})
	if err != nil {
	    // TODO: Handle error.
	}

We'll see how to create a table with a schema in the next section.

# Schemas

There are two ways to construct schemas with this package.
You can build a schema by hand with the [Schema] struct, like so:

	schema1 := bigquery.Schema{
	    {Name: "Name", Required: true, Type: bigquery.StringFieldType},
	    {Name: "Grades", Repeated: true, Type: bigquery.IntegerFieldType},
	    {Name: "Optional", Required: false, Type: bigquery.IntegerFieldType},
	}

Or you can infer the schema from a struct with the [InferSchema] method:

	type student struct {
	    Name   string
	    Grades []int
	    Optional bigquery.NullInt64
	}
	schema2, err := bigquery.InferSchema(student{})
	if err != nil {
	    // TODO: Handle error.
	}
	// schema1 and schema2 are identical.

Struct inference supports tags like those of the [encoding/json] package, so you can
change names, ignore fields, or mark a field as nullable (non-required). Fields
declared as one of the Null types ([NullInt64], [NullFloat64], [NullString], [NullBool],
[NullTimestamp], [NullDate], [NullTime], [NullDateTime], [NullGeography], and [NullJSON]) are
automatically inferred as nullable, so the "nullable" tag is only needed for []byte,
*big.Rat, *[IntervalValue], *[RangeValue], and pointer-to-struct fields.

	type student2 struct {
	    Name     string `bigquery:"full_name"`
	    Grades   []int
	    Secret   string `bigquery:"-"`
	    Optional []byte `bigquery:",nullable"`
	}
	schema3, err := bigquery.InferSchema(student2{})
	if err != nil {
	    // TODO: Handle error.
	}
	// schema3 has required field "full_name", repeated field "Grades", and nullable BYTES field "Optional".

Having constructed a schema, you can create a table with it using the [Table.Create] method like so:

	if err := table.Create(ctx, &bigquery.TableMetadata{Schema: schema1}); err != nil {
	    // TODO: Handle error.
	}

# Copying

You can copy one or more tables to another table. Begin by constructing a [Copier]
describing the copy using the [Table.CopierFrom]. Then set any desired copy options,
and finally call [Copier.Run] to get a [Job]:

	copier := myDataset.Table("dest").CopierFrom(myDataset.Table("src"))
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err = copier.Run(ctx)
	if err != nil {
	    // TODO: Handle error.
	}

You can chain the call to [Copier.Run] if you don't want to set options:

	job, err = myDataset.Table("dest").CopierFrom(myDataset.Table("src")).Run(ctx)
	if err != nil {
	    // TODO: Handle error.
	}

You can wait for your job to complete with the [Job.Wait] method:

	status, err := job.Wait(ctx)
	if err != nil {
	    // TODO: Handle error.
	}
	if err := status.Err(); err != nil {
	    // TODO: Handle error.
	}

[Job.Wait] polls with exponential backoff. You can also poll yourself, if you
wish:

	for {
	    status, err := job.Status(ctx)
	    if err != nil {
	        // TODO: Handle error.
	    }
	    if status.Done() {
	        if status.Err() != nil {
	            log.Fatalf("Job failed with error %v", status.Err())
	        }
	        break
	    }
	    time.Sleep(pollInterval)
	}

# Loading and Uploading

There are two ways to populate a table with this package: load the data from a Google Cloud Storage
object, or upload rows directly from your program.

For loading, first create a [GCSReference] with the [NewGCSReference] method, configuring it if desired.
Then make a [Loader] from a table with the [Table.LoaderFrom] method with the reference,
optionally configure it as well, and call its [Loader.Run] method.

	gcsRef := bigquery.NewGCSReference("gs://my-bucket/my-object")
	gcsRef.AllowJaggedRows = true
	loader := myDataset.Table("dest").LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever
	job, err = loader.Run(ctx)
	// Poll the job for completion if desired, as above.

To upload, first define a type that implements the [ValueSaver] interface, which has
a single method named Save. Then create an [Inserter], and call its [Inserter.Put]
method with a slice of values.

	type Item struct {
		Name  string
		Size  float64
		Count int
	}

	// Save implements the ValueSaver interface.
	func (i *Item) Save() (map[string]bigquery.Value, string, error) {
		return map[string]bigquery.Value{
			"Name":  i.Name,
			"Size":  i.Size,
			"Count": i.Count,
		}, "", nil
	}

	u := table.Inserter()
	// Item implements the ValueSaver interface.
	items := []*Item{
	    {Name: "n1", Size: 32.6, Count: 7},
	    {Name: "n2", Size: 4, Count: 2},
	    {Name: "n3", Size: 101.5, Count: 1},
	}
	if err := u.Put(ctx, items); err != nil {
	    // TODO: Handle error.
	}

You can also upload a struct that doesn't implement [ValueSaver]. Use the [StructSaver] type
to specify the schema and insert ID by hand:

	type item struct {
		Name string
		Num  int
	}

	// Assume schema holds the table's schema.
	savers := []*bigquery.StructSaver{
		{Struct: item{Name: "n1", Num: 12}, Schema: schema, InsertID: "id1"},
		{Struct: item{Name: "n2", Num: 31}, Schema: schema, InsertID: "id2"},
		{Struct: item{Name: "n3", Num: 7}, Schema: schema, InsertID: "id3"},
	}

	if err := u.Put(ctx, savers); err != nil {
	    // TODO: Handle error.
	}

Lastly, but not least, you can just supply the struct or struct pointer directly and the schema will be inferred:

	type Item2 struct {
	    Name  string
	    Size  float64
	    Count int
	}

	// Item2 doesn't implement ValueSaver interface, so schema will be inferred.
	items2 := []*Item2{
	    {Name: "n1", Size: 32.6, Count: 7},
	    {Name: "n2", Size: 4, Count: 2},
	    {Name: "n3", Size: 101.5, Count: 1},
	}
	if err := u.Put(ctx, items2); err != nil {
	    // TODO: Handle error.
	}

BigQuery allows for higher throughput when omitting insertion IDs.  To enable this,
specify the sentinel [NoDedupeID] value for the insertion ID when implementing a [ValueSaver].

# Extracting

If you've been following so far, extracting data from a BigQuery table
into a Google Cloud Storage object will feel familiar. First create an
[Extractor], then optionally configure it, and lastly call its [Extractor.Run] method.

	extractor := table.ExtractorTo(gcsRef)
	extractor.DisableHeader = true
	job, err = extractor.Run(ctx)
	// Poll the job for completion if desired, as above.

# Errors

Errors returned by this package come from distinct surfaces depending on the
operation:

  - Synchronous REST RPC failures typically wrap
    *[google.golang.org/api/googleapi.Error].
  - Asynchronous job failures returned by [JobStatus.Err] and [JobStatus.Errors]
    use *[Error]. Match them with [errors.As] using a pointer target
    (var bqErr *bigquery.Error).
  - Streaming row rejections from [Inserter.Put] return [PutMultiError]
    (containing [RowInsertionError] entries with [MultiError] slices). None of
    these wrapper types implement Unwrap, so extract [PutMultiError] with
    [errors.As] and iterate its elements directly.
  - Storage Read API failures (when [Client.EnableStorageReadClient] is enabled)
    wrap gRPC status errors.
  - Authentication and token-fetch failures may originate from
    golang.org/x/oauth2 rather than *[google.golang.org/api/googleapi.Error].

Use [errors.As] to inspect REST API errors:

	var e *googleapi.Error
	if errors.As(err, &e) {
		if e.Code == 409 { ... }
	}

In some cases, your client may receive unstructured
*[google.golang.org/api/googleapi.Error] responses when exceeding BigQuery
request limits. See https://docs.cloud.google.com/bigquery/docs/errors for API
HTTP status and error reason codes,
https://docs.cloud.google.com/bigquery/docs/error-messages for client and
connectivity errors, and https://docs.cloud.google.com/bigquery/quotas for
request limits.

# Retries and Timeouts

Most RPCs automatically retry transient network and transport errors, HTTP 5xx
responses (500, 502, 503, and 504), and selected structured BigQuery error
reasons (such as backendError and rateLimitExceeded, plus jobRateLimitExceeded
and internalError when enqueuing jobs or polling query results via [Job.Wait]
and [Job.Read]) using hardcoded exponential backoff (gax.Backoff with Initial:
1s, Max: 32s, Multiplier: 2) until the context is canceled or expires. Polling
retries do not rerun a failed job. Always pass a context with a deadline or
timeout, because retries have no fixed maximum attempt limit.

Automatic retries do not apply to unstructured HTTP 429 responses (without a
structured rateLimitExceeded reason), quotaExceeded errors, loads from a
[ReaderSource], or [Dataset.Create], [Routine.Create], [Model.Delete], and
[Routine.Delete].

When implementing an outer retry loop for errors the client does not
automatically retry (such as unstructured HTTP 429 responses or terminal job
errors like jobBackendError, jobInternalError, and jobRateLimitExceeded that
require submitting a new job), pass the overall deadline context to [Query.Run]
and other RPCs rather than a short per-attempt context timeout. Canceling a call
on a short per-attempt timeout resets the SDK's internal backoff back to 1s on
every outer attempt, amplifying request volume under sustained throttling or
errors. Use outer exponential backoff (Initial >= 1s, Max >= 32s) when
resubmitting failed jobs or retrying unstructured HTTP 429 responses.
*/
package bigquery // import "cloud.google.com/go/bigquery"
