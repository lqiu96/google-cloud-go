// Copyright 2016 Google LLC
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
Package datastore provides a client for Google Cloud Datastore (and Cloud
Firestore in Datastore mode).

See https://pkg.go.dev/cloud.google.com/go for authentication, timeouts,
connection pooling, and similar aspects of this package.

# Choosing a Package

  - cloud.google.com/go/datastore (this package): Data-plane client for Cloud
    Datastore and Firestore in Datastore mode.
  - cloud.google.com/go/datastore/apiv1/datastorepb: Protocol Buffer message
    types only (there is no generated datastore/apiv1 Go client). Import this
    package when unpacking *datastorepb.Value values from [AggregationResult].
  - cloud.google.com/go/datastore/admin/apiv1: Generated admin client for
    exports, imports, and composite index management.
  - cloud.google.com/go/firestore: Use this package instead for databases in
    Firestore Native mode (see
    https://docs.cloud.google.com/datastore/docs/firestore-or-datastore).

# Client Lifecycle and Concurrency

Create a single [Client] per (projectID, databaseID) pair and reuse it across
the application. Call [Client.Close] when finished to release the gRPC
connection pool. Use [NewClient] for the default database ([DefaultDatabaseID],
"") or [NewClientWithDatabase] for a named database. Unlike Cloud Firestore,
"(default)" is rejected by the Datastore API.

Do not cancel or set a short timeout on the context passed to [NewClient] or
[NewClientWithDatabase], as it governs background credential setup for the
client's lifetime.

Methods on [Client] are safe for concurrent use by multiple goroutines, except
[Client.WithReadOptions] and [Client.Close]. [Transaction], [Iterator], and
[AggregationQuery] are not safe for concurrent use.

# Basic Operations

Entities are the unit of storage and are associated with a [Key]. A key
consists of an optional parent key, an optional namespace, a string kind
(also known as an entity type), and either a string Name or an integer ID.

It is valid to create a key with a zero Name and a zero ID. This is called an
incomplete key and does not refer to any saved entity. Putting an entity into
Datastore under an incomplete key generates a unique complete key with a
non-zero ID (the input key is not mutated).

An entity's contents are a mapping from case-sensitive field names to values.
Valid struct field types are:
  - Signed integers (int, int8, int16, int32, and int64)
  - bool
  - string (must be valid UTF-8)
  - float32 and float64
  - []byte (up to 1 megabyte in length, indexed by default up to 1,500 bytes)
  - Any type whose underlying type is one of the above predeclared types
  - *Key
  - GeoPoint
  - time.Time (stored with microsecond precision, retrieved as UTC)
  - interface{} (holding a supported value type or nil)
  - Structs whose fields are all valid value types
  - Pointers to structs whose fields are all valid value types
  - Slices of any of the above (except slices of slices other than [][]byte)
  - Pointers to a signed integer, bool, string, float32, float64, time.Time,
    GeoPoint, or []byte

Unsigned integer types (uint, uint8, uint16, uint32, uint64) and Go map types
are not supported as struct fields. Use [PropertyList] or implement
[PropertyLoadSaver] for dynamic or map-like entities.

Slices of structs are valid, as are structs that contain slices.

[Client.Get] and [Client.Put] load and save an entity's contents. An entity's
contents are typically represented by a struct pointer.

Example code:

	type Entity struct {
		Value string
	}

	func main() {
		ctx := context.Background()

		// Create a datastore client. In a typical application, you would create
		// a single client which is reused for every datastore operation.
		dsClient, err := datastore.NewClient(ctx, "my-project")
		if err != nil {
			// Handle error.
		}
		defer dsClient.Close()

		k := datastore.NameKey("Entity", "stringID", nil)
		e := new(Entity)
		if err := dsClient.Get(ctx, k, e); err != nil {
			// Handle error.
		}

		old := e.Value
		e.Value = "Hello World!"

		if _, err := dsClient.Put(ctx, k, e); err != nil {
			// Handle error.
		}

		fmt.Printf("Updated value from %q to %q\n", old, e.Value)
	}

[Client.GetMulti], [Client.PutMulti], and [Client.DeleteMulti] are batch
versions of Get, Put, and Delete. They take a []*Key instead of a *Key and may
return a [MultiError] when encountering per-entity failures.

[Client.Mutate] generalizes PutMulti and DeleteMulti to a sequence of any
Datastore mutations created with [NewInsert], [NewUpdate], [NewUpsert], and
[NewDelete]. Client.Mutate uses non-transactional mode. If atomicity is
required, use [Transaction.Mutate] instead.

# Properties

An entity's contents can be represented by a variety of types. These are
typically struct pointers, but can also be any type that implements the
[PropertyLoadSaver] interface. If using a struct pointer, you do not have to
explicitly implement PropertyLoadSaver. The datastore package automatically
converts via reflection. If a struct pointer does implement PropertyLoadSaver,
those methods are used in preference to the default behavior for struct
pointers. Struct pointers are more strongly typed and easier to use.
PropertyLoadSavers are more flexible.

The actual types passed do not have to match between Get and Put calls or even
across different calls to datastore. It is valid to put a *PropertyList and
get that same entity as a *myStruct, or put a *myStruct0 and get a *myStruct1.
Conceptually, any entity is saved as a sequence of properties, and is loaded
into the destination value on a property-by-property basis. When loading into
a struct pointer, an entity that cannot be completely represented (such as a
missing field or type mismatch) returns an *[ErrFieldMismatch] error while
still loading all other matching fields. If multiple fields mismatch, only one
non-deterministic mismatch is reported. Callers may ignore ErrFieldMismatch
individually or pass [WithIgnoreFieldMismatch] to [NewClient] for Client reads.

By default, for struct pointers, all exported fields are indexed, and the
property name is the same as the field name (unexported fields are skipped on
save).

Fields may have a `datastore:"name,options"` tag. The tag name is the
property name, which must be one or more valid Go identifiers joined by ".",
but may start with a lower case letter. An empty tag name means to just use the
field name. A "-" tag name means that the datastore will ignore that field.

The only valid options are "omitempty", "noindex" and "flatten".

If the options include "omitempty" and the value of the field is an empty
value, then the field will be omitted on Save. Empty values are defined as
false, 0, a nil pointer, a nil interface value, the zero time.Time, and any
empty slice or string. (Empty slices are never saved, even without
"omitempty".) Other structs, including GeoPoint, are never considered empty.

If options include "noindex" then the field will not be indexed. All fields
are indexed by default, including []byte (unlike legacy App Engine Datastore,
there is no separate ByteString type, and []byte values up to 1,500 bytes can
be filtered in queries directly). Indexed string and []byte properties are
limited to 1,500 bytes, strings must be valid UTF-8, and an entity may have at
most 20,000 indexed properties (counting each element of an indexed slice).
Tag fields holding longer strings or binary blobs with `datastore:"<name>,noindex"`
(for example, `datastore:"body,noindex"`) or `datastore:",noindex"` (with a
comma before "noindex"). An empty name before the comma (`datastore:",noindex"`)
stores the property under the Go exported field name (for example, "Body", not
"body"), so when using a custom or lowercase property name you must keep the
name before the comma (`datastore:"body,noindex"`). Omitting the comma entirely
(`datastore:"noindex"`) renames the property to "noindex" and leaves it indexed.

For a nested struct field, the options may also include "flatten". This
indicates that the immediate fields and any nested substruct fields of the
nested struct should be flattened. See below for examples.

To use multiple options together, separate them by a comma.
The order does not matter.

If the options is "" then the comma may be omitted.

Example code:

	// A and B are renamed to a and b.
	// A, C and J are not indexed (C is stored as "C"; J is stored as "j" in both datastore and json).
	// D's tag is equivalent to having no tag at all (E).
	// I is ignored entirely by the datastore.
	// J has tag information for both the datastore and json packages.
	type TaggedStruct struct {
		A int `datastore:"a,noindex"`
		B int `datastore:"b"`
		C int `datastore:",noindex"`
		D int `datastore:""`
		E int
		I int `datastore:"-"`
		J int `datastore:"j,noindex" json:"j"`
	}

# Slice Fields

A field of slice type corresponds to a Datastore array property, except for []byte, which corresponds
to a Datastore blob.

Zero-length slice fields are not saved. Slice fields of length 1 or greater are saved
as Datastore arrays. When a zero-length Datastore array is loaded into a slice field,
the slice field remains unchanged.

If a non-array value is loaded into a slice field, the result will be a slice with
one element, containing the value.

# Loading Nulls

Loading a Datastore Null into a basic type (int, float, etc.) results in a zero value.
Loading a Null into a slice of basic type results in a slice of size 1 containing the zero value.
Loading a Null into a pointer field results in nil.
Loading a Null into a field of struct type is an error.

# Pointer Fields

A struct field can be a pointer to a signed integer, floating-point number,
string, bool, time.Time, GeoPoint, or []byte. Putting a non-nil pointer stores
its dereferenced value. Putting a nil pointer of one of these types (or a nil
*Key or nil interface{}) stores a Datastore Null property, unless the field is
marked omitempty, in which case no property is stored. In contrast, a nil
pointer to a struct is always omitted on save (never stored as Null), even
without omitempty.

Loading a Null into a pointer field sets the pointer to nil. Loading any other value
allocates new storage with the value, and sets the field to point to it.

# Key Field

If the struct contains a *datastore.Key field tagged with the name "__key__",
its value will be ignored on Put. When reading the Entity back into the Go struct,
the field will be populated with the *datastore.Key value used to query for
the Entity.

Example code:

	type MyEntity struct {
		A int
		K *datastore.Key `datastore:"__key__"`
	}

	func main() {
		ctx := context.Background()
		dsClient, err := datastore.NewClient(ctx, "my-project")
		if err != nil {
			// Handle error.
		}
		defer dsClient.Close()

		k := datastore.NameKey("Entity", "stringID", nil)
		e := MyEntity{A: 12}
		if _, err := dsClient.Put(ctx, k, &e); err != nil {
			// Handle error.
		}

		var entities []MyEntity
		q := datastore.NewQuery("Entity").FilterField("A", "=", 12).Limit(1)
		if _, err := dsClient.GetAll(ctx, q, &entities); err != nil {
			// Handle error
		}

		log.Println(entities[0])
		// Prints {12 /Entity,stringID}
	}

# Structured Properties

If the struct pointed to contains other structs, then the nested or embedded
structs are themselves saved as Entity values. For example, given these definitions:

	type Inner struct {
		W int32
		X string
	}

	type Outer struct {
		I Inner
	}

then an Outer would have one property, I, encoded as an Entity value.

Note: embedded struct fields must be named to be encoded as an Entity. For
example, in case of a type Outer with an anonymous embedded field Inner:

	type Outer struct {
		Inner
	}

all the exported Inner struct fields are promoted and saved as top-level
properties of Outer itself. Warning: if an anonymously embedded struct
implements [PropertyLoadSaver], the outer struct inherits its Load and Save
methods and silently drops the outer struct's own fields unless the outer
struct implements Load and Save explicitly.

If an outer struct field is tagged with the "noindex" option (for example,
`datastore:"<name>,noindex"` or `datastore:",noindex"`) then all of its
implicit flattened fields are effectively "noindex".

If the Inner struct contains a *Key field with the name "__key__", like so:

	type Inner struct {
		W int32
		X string
		K *datastore.Key `datastore:"__key__"`
	}

	type Outer struct {
		I Inner
	}

then the value of K will be used as the Key for Inner, represented
as an Entity value in datastore.

If any nested struct fields should be flattened, instead of encoded as
Entity values, the nested struct field should be tagged with the "flatten"
option. For example, given the following:

	type Inner1 struct {
		W int32
		X string
	}

	type Inner2 struct {
		Y float64
	}

	type Inner3 struct {
		Z bool
	}

	type Inner4 struct {
		WW int
	}

	type Inner5 struct {
		X Inner4
	}

	type Outer struct {
		A int16
		I []Inner1 `datastore:",flatten"`
		J Inner2   `datastore:",flatten"`
		K Inner5   `datastore:",flatten"`
		Inner3     `datastore:",flatten"`
	}

an Outer's properties would be equivalent to those of:

	type OuterEquivalent struct {
		A          int16
		IDotW      []int32  `datastore:"I.W"`
		IDotX      []string `datastore:"I.X"`
		JDotY      float64  `datastore:"J.Y"`
		KDotXDotWW int      `datastore:"K.X.WW"`
		Z          bool
	}

The "flatten" option can be used on nested structs and [PropertyLoadSaver]
fields (nested PropertyLoadSaver implementations must use pointer receivers).
Flattened structs cannot produce slices of slices (except [][]byte). Avoid
combining "omitempty" with flattened slices of structs, because omitting fields
on only some slice elements causes save errors or misaligned parallel arrays.

# The PropertyLoadSaver Interface

An entity's contents can also be represented by any type that implements the
PropertyLoadSaver interface. This type may be a struct pointer, but it does
not have to be. The datastore package will call Load when getting the entity's
contents, and Save when putting the entity's contents.
Possible uses include deriving non-stored fields, verifying fields, or indexing
a field only if its value is positive.

Example code:

	type CustomPropsExample struct {
		I, J int
		// Sum is not stored, but should always be equal to I + J.
		Sum int `datastore:"-"`
	}

	func (x *CustomPropsExample) Load(ps []datastore.Property) error {
		// Load I and J as usual.
		if err := datastore.LoadStruct(x, ps); err != nil {
			return err
		}
		// Derive the Sum field.
		x.Sum = x.I + x.J
		return nil
	}

	func (x *CustomPropsExample) Save() ([]datastore.Property, error) {
		// Validate the Sum field.
		if x.Sum != x.I + x.J {
			return nil, errors.New("CustomPropsExample has inconsistent sum")
		}
		// Save I and J as usual. The code below is equivalent to calling
		// "return datastore.SaveStruct(x)", but is done manually for
		// demonstration purposes.
		return []datastore.Property{
			{
				Name:  "I",
				Value: int64(x.I),
			},
			{
				Name:  "J",
				Value: int64(x.J),
			},
		}, nil
	}

The *PropertyList type implements PropertyLoadSaver, and can therefore hold an
arbitrary entity's contents.

# The KeyLoader Interface

If a type implements the [PropertyLoadSaver] interface, it may also want to
implement the [KeyLoader] interface. The KeyLoader interface allows
implementations of PropertyLoadSaver to load an entity's [Key] into the Go
type. This type may be a struct pointer, but it does not have to be. The
datastore package calls LoadKey before calling Load when getting the entity's
contents. If LoadKey returns an error, Load is still called, and the error from
LoadKey is returned.

Example code:

	type WithKeyExample struct {
		I int
		Key   *datastore.Key
	}

	func (x *WithKeyExample) LoadKey(k *datastore.Key) error {
		x.Key = k
		return nil
	}

	func (x *WithKeyExample) Load(ps []datastore.Property) error {
		// Load I as usual.
		return datastore.LoadStruct(x, ps)
	}

	func (x *WithKeyExample) Save() ([]datastore.Property, error) {
		// Save I as usual.
		return datastore.SaveStruct(x)
	}

To load a Key into a struct which does not implement the PropertyLoadSaver
interface, see the "Key Field" section above.

# Queries

Queries retrieve entities based on their properties or key's ancestry. Running
a query yields an [Iterator] of results: either keys or (key, entity) pairs.
Queries are reusable, and it is safe to call [Client.Run] from concurrent
goroutines with the same [Query]. Iterators are not safe for concurrent use.

Queries are immutable, and are either created by calling [NewQuery], or derived
from an existing query by calling a method like [Query.FilterField],
[Query.FilterEntity], or [Query.Order] that returns a new query value. A query
is typically constructed by calling NewQuery followed by a chain of zero or
more such methods. These methods are:
  - Ancestor, FilterField, and FilterEntity constrain the entities returned by
    running a query.
  - Order affects the order in which they are returned.
  - Project constrains the fields returned.
  - Distinct de-duplicates projected entities.
  - KeysOnly makes the iterator return only keys, not (key, entity) pairs.
  - Start, End, Offset and Limit define which sub-sequence of matching entities
    to return. Start and End take cursors, Offset and Limit take integers. Start
    and Offset affect the first result, End and Limit affect the last result.
    If both Start and Offset are set, then the offset is relative to Start.
    If both End and Limit are set, then the earliest constraint wins. Limit is
    relative to Start+Offset, not relative to End. As a special case, a
    negative limit means unlimited.

Example code:

	type Widget struct {
		Description string
		Price       int
	}

	func printWidgets(ctx context.Context, client *datastore.Client) {
		q := datastore.NewQuery("Widget").
			FilterField("Price", "<", 1000).
			Order("-Price")

		t := client.Run(ctx, q)
		for {
			var x Widget
			key, err := t.Next(&x)
			if err == iterator.Done {
				break
			}
			if err != nil {
				// Handle error.
			}
			fmt.Printf("Key=%v\nWidget=%#v\n\n", key, x)
		}
	}

# Transactions

[Client.RunInTransaction] runs a callback function in a transaction and commits
on a nil return. By default it retries up to 3 times (configurable with
[MaxAttempts]) on transient gRPC errors including Aborted
([ErrConcurrentTransaction]), DeadlineExceeded, Internal, Canceled, Unknown,
and Unauthenticated, rolling back automatically on retryable errors,
non-retryable errors, and panics. Keep the callback idempotent and reset any
outer variables on each attempt.

Inside a transaction, perform all reads and writes via the [Transaction]
methods or [Query.Transaction] rather than standalone [Client] methods. Writes
are buffered locally until commit and are not visible to reads or queries in
the same transaction.

Example code:

	type Counter struct {
		Count int
	}

	func incCount(ctx context.Context, client *datastore.Client) {
		var count int
		key := datastore.NameKey("Counter", "singleton", nil)
		_, err := client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
			var x Counter
			if err := tx.Get(key, &x); err != nil && err != datastore.ErrNoSuchEntity {
				return err
			}
			x.Count++
			if _, err := tx.Put(key, &x); err != nil {
				return err
			}
			count = x.Count
			return nil
		})
		if err != nil {
			// Handle error.
		}
		// The value of count is only valid once the transaction is successful
		// (RunInTransaction has returned nil).
		fmt.Printf("Count=%d\n", count)
	}

Pass the [ReadOnly] option to [Client.RunInTransaction] if your transaction is
used only for Get, GetMulti, or queries. Read-only transactions are more
efficient and still retry on transient errors. Enqueued writes on a ReadOnly
transaction fail at commit time.

# Errors and Retries

Datastore operations can return package sentinel errors ([ErrNoSuchEntity],
[ErrInvalidKey], [ErrInvalidEntityType], [ErrConcurrentTransaction], and
*[ErrFieldMismatch]), [MultiError] for batch per-entity failures, or raw gRPC
status errors (from google.golang.org/grpc/status, such as codes.AlreadyExists
or codes.NotFound).

[MultiError] does not implement Unwrap, so errors.Is and errors.As do not match
element errors inside a MultiError. Extract it with errors.As(err, &me) and
inspect each me[i] directly. Whole-call RPC and context errors are returned
directly rather than as MultiError.

By default, the gRPC service config sets a 60-second RPC timeout with
waitForReady enabled and up to 5 automatic retry attempts on UNAVAILABLE for
all RPCs (including non-transactional writes) and on DEADLINE_EXCEEDED,
INTERNAL, and RESOURCE_EXHAUSTED for reads and ID reservations (Lookup,
RunQuery, RunAggregationQuery, and ReserveIds). Pass a shorter context deadline
for fail-fast calls.

# Google Cloud Datastore Emulator

This package supports the Cloud Datastore emulator, which is useful for testing
and development. Setting the DATASTORE_EMULATOR_HOST environment variable
directs traffic to the emulator instead of production Cloud Datastore,
disabling authentication and using insecure gRPC credentials automatically.

To install and set up the emulator and its environment variables, see the
documentation at
https://docs.cloud.google.com/datastore/docs/tools/datastore-emulator.
For Firestore in Datastore mode, start the Firestore emulator in Datastore mode
instead of the legacy Cloud Datastore emulator:

	gcloud emulators firestore start --database-mode=datastore-mode

When DATASTORE_EMULATOR_HOST is set, passing [DetectProjectID] falls back to
"dummy-emulator-datastore-project" if credential detection fails, whereas
passing "" without DATASTORE_PROJECT_ID still returns an error. When running
the emulator in Docker, bind it to 0.0.0.0 rather than localhost so other
containers can reach it. Set DATASTORE_EMULATOR_HOST to host:port (using
bracketed syntax for IPv6 addresses, such as [::1]:8081) without an http://
scheme.

When connecting to production Cloud Datastore from minimal container images
(such as scratch or debian-slim), install root CA certificates
(ca-certificates) so TLS handshakes succeed.

To use the emulator with this library, set DATASTORE_EMULATOR_HOST and create a
client as usual:

	// Set DATASTORE_EMULATOR_HOST environment variable.
	err := os.Setenv("DATASTORE_EMULATOR_HOST", "localhost:9000")
	if err != nil {
		// TODO: Handle error.
	}
	// Create client as usual.
	client, err := datastore.NewClient(ctx, "my-project-id")
	if err != nil {
		// TODO: Handle error.
	}
	defer client.Close()
*/
package datastore // import "cloud.google.com/go/datastore"
