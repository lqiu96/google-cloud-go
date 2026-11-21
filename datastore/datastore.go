// Copyright 2014 Google LLC
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

package datastore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"cloud.google.com/go/internal/trace"
	"google.golang.org/api/option"
	"google.golang.org/api/option/internaloption"
	"google.golang.org/api/transport"
	gtransport "google.golang.org/api/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	prodEndpointTemplate = "datastore.UNIVERSE_DOMAIN:443"
	prodUniverseDomain   = "googleapis.com"
	prodMtlsAddr         = "datastore.mtls.googleapis.com:443"
	userAgent            = "gcloud-golang-datastore/20160401"
)

// retryPolicy is based on https://github.com/googleapis/googleapis/blob/master/google/datastore/v1/datastore_grpc_service_config.json
// but updated to include INTERNAL and RESOURCE_EXHAUSTED for the data plane.
const retryPolicy = `{
  "methodConfig": [
    {
      "name": [{"service": "google.datastore.v1.Datastore"}],
      "timeout": "60s",
			"waitForReady": true,
      "retryPolicy": {
        "maxAttempts": 5,
        "initialBackoff": "0.100s",
        "maxBackoff": "60s",
        "backoffMultiplier": 1.3,
        "retryableStatusCodes": ["UNAVAILABLE"]
      }
    },
    {
      "name": [
        {"service": "google.datastore.v1.Datastore", "method": "Lookup"},
        {"service": "google.datastore.v1.Datastore", "method": "RunQuery"},
        {"service": "google.datastore.v1.Datastore", "method": "RunAggregationQuery"},
        {"service": "google.datastore.v1.Datastore", "method": "ReserveIds"}
      ],
      "timeout": "60s",
			"waitForReady": true,
      "retryPolicy": {
        "maxAttempts": 5,
        "initialBackoff": "0.100s",
        "maxBackoff": "60s",
        "backoffMultiplier": 1.3,
        "retryableStatusCodes": ["UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL", "RESOURCE_EXHAUSTED"]
      }
    }
  ]
}`

// ScopeDatastore grants permissions to view and/or manage datastore entities
const ScopeDatastore = "https://www.googleapis.com/auth/datastore"

// DetectProjectID is a sentinel value that instructs [NewClient] and
// [NewClientWithDatabase] to detect the project ID from the provided
// credentials or Application Default Credentials
// (https://docs.cloud.google.com/docs/authentication/application-default-credentials).
// Not all credential types include a project ID (for example, JWT credentials).
//
// When DATASTORE_EMULATOR_HOST is set, DetectProjectID ignores credential
// detection errors and falls back to "dummy-emulator-datastore-project". Passing
// "" instead reads DATASTORE_PROJECT_ID and fails if that variable is unset.
const DetectProjectID = "*detect-project-id*"

// resourcePrefixHeader is the name of the metadata header used to indicate
// the resource being operated on.
const resourcePrefixHeader = "google-cloud-resource-prefix"

// reqParamsHeader is routing header required to access named databases
const reqParamsHeader = "x-goog-request-params"

// DefaultDatabaseID is the ID of the default Datastore database, represented by
// an empty string (""). Unlike Cloud Firestore, "(default)" is rejected by the
// Datastore API.
const DefaultDatabaseID = ""

var (
	gtransportDialPoolFn = gtransport.DialPool
	detectProjectIDFn    = detectProjectID
	schemeRegexp         = regexp.MustCompile("^(http://|https://|passthrough:///)")
)

// Client is a client for reading and writing data in a Cloud Datastore (or
// Firestore in Datastore mode) database over gRPC. Each Client targets a single
// (projectID, databaseID) pair. Create a separate Client for each database.
//
// Clients should be reused instead of created per request. Methods on Client are
// safe for concurrent use by multiple goroutines, except [Client.WithReadOptions]
// and [Client.Close]. [Transaction], [Iterator], and [AggregationQuery] are not
// safe for concurrent use. Call [Client.Close] when finished.
type Client struct {
	connPool     gtransport.ConnPool
	client       pb.DatastoreClient
	dataset      string // Called dataset by the datastore API, synonym for project ID.
	databaseID   string // Default value is empty string
	readSettings *readSettings
	config       *datastoreConfig
}

// NewClient creates a new [Client] connected to the default database
// ([DefaultDatabaseID]). Use [NewClientWithDatabase] to connect to a named
// database.
//
// If projectID is "", it is read from the DATASTORE_PROJECT_ID environment
// variable (failing if unset, even when using the emulator). Pass
// [DetectProjectID] to detect the project ID from credentials (falling back to
// "dummy-emulator-datastore-project" when DATASTORE_EMULATOR_HOST is set).
//
// If DATASTORE_EMULATOR_HOST is set, the client connects to that address with
// authentication disabled and insecure gRPC credentials. Do not cancel or set a
// short timeout on ctx, as it governs background credential setup for the
// client's lifetime.
//
// By default, RPCs use a 60-second timeout with waitForReady enabled and
// automatic retries (up to 5 attempts) on UNAVAILABLE for all RPCs (including
// non-transactional writes) and on DEADLINE_EXCEEDED, INTERNAL, and
// RESOURCE_EXHAUSTED for reads and ID reservations (Lookup, RunQuery,
// RunAggregationQuery, and ReserveIds). Pass a shorter per-call context
// deadline for fail-fast calls. Call [Client.Close] when finished.
func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	client, err := NewClientWithDatabase(ctx, projectID, DefaultDatabaseID, opts...)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// NewClientWithDatabase creates a new [Client] for the given project and
// database. Pass [DefaultDatabaseID] ("") for the default database. Unlike
// Cloud Firestore, "(default)" is rejected by the Datastore API.
//
// If projectID is "", it is read from DATASTORE_PROJECT_ID (failing if unset).
// Pass [DetectProjectID] to detect the project ID from credentials (falling back
// to "dummy-emulator-datastore-project" when DATASTORE_EMULATOR_HOST is set).
//
// If DATASTORE_EMULATOR_HOST is set, authentication is disabled and insecure
// gRPC credentials are used automatically. Do not cancel or set a short timeout
// on ctx, as it governs background credential setup for the client's lifetime.
// See [NewClient] for default RPC timeout and retry behavior. Call [Client.Close]
// when finished.
func NewClientWithDatabase(ctx context.Context, projectID, databaseID string, opts ...option.ClientOption) (*Client, error) {
	var o []option.ClientOption
	keepaliveParams := keepalive.ClientParameters{
		Time:                1 * time.Minute,
		Timeout:             20 * time.Second,
		PermitWithoutStream: true,
	}

	// Environment variables for gcd emulator:
	// https://docs.cloud.google.com/datastore/docs/tools/datastore-emulator
	// If the emulator is available, dial it without passing any credentials.
	if addr := os.Getenv("DATASTORE_EMULATOR_HOST"); addr != "" {
		// If it's already a valid gRPC target with a non-HTTP scheme (like unix:///), leave it.
		// Otherwise, strip legacy schemes and force passthrough for performance.
		if !(strings.Contains(addr, "://") && !strings.HasPrefix(addr, "http")) {
			addr = schemeRegexp.ReplaceAllString(addr, "")
			addr = "passthrough:///" + addr
		}
		o = []option.ClientOption{
			option.WithEndpoint(addr),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithGRPCDialOption(grpc.WithDefaultServiceConfig(retryPolicy)),
			option.WithGRPCDialOption(grpc.WithKeepaliveParams(keepaliveParams)),
		}
		if projectID == DetectProjectID {
			projectID, _ = detectProjectIDFn(ctx, opts...)
			if projectID == "" {
				projectID = "dummy-emulator-datastore-project"
			}
		}
	} else {
		o = []option.ClientOption{
			internaloption.WithDefaultEndpointTemplate(prodEndpointTemplate),
			internaloption.WithDefaultUniverseDomain(prodUniverseDomain),
			internaloption.WithDefaultMTLSEndpoint(prodMtlsAddr),
			internaloption.EnableNewAuthLibrary(),
			option.WithScopes(ScopeDatastore),
			option.WithUserAgent(userAgent),
			// Add the Service Config for retries
			option.WithGRPCDialOption(grpc.WithDefaultServiceConfig(retryPolicy)),
			// Add Keepalives to prune zombie connections
			option.WithGRPCDialOption(grpc.WithKeepaliveParams(keepaliveParams)),
		}
	}
	// Warn if we see the legacy emulator environment variables.
	if os.Getenv("DATASTORE_HOST") != "" && os.Getenv("DATASTORE_EMULATOR_HOST") == "" {
		log.Print("WARNING: legacy environment variable DATASTORE_HOST is ignored. Use DATASTORE_EMULATOR_HOST instead.")
	}
	if os.Getenv("DATASTORE_DATASET") != "" && os.Getenv("DATASTORE_PROJECT_ID") == "" {
		log.Print("WARNING: legacy environment variable DATASTORE_DATASET is ignored. Use DATASTORE_PROJECT_ID instead.")
	}
	if projectID == "" {
		projectID = os.Getenv("DATASTORE_PROJECT_ID")
	}

	o = append(o, opts...)

	if projectID == DetectProjectID {
		detected, err := detectProjectIDFn(ctx, opts...)
		if err != nil {
			return nil, err
		}
		projectID = detected
	}

	if projectID == "" {
		return nil, errors.New("datastore: missing project/dataset id")
	}
	connPool, err := gtransportDialPoolFn(ctx, o...)
	if err != nil {
		return nil, fmt.Errorf("dialing: %w", err)
	}

	config := newDatastoreConfig(o...)
	return &Client{
		connPool:     connPool,
		client:       newDatastoreClient(connPool, projectID, databaseID),
		dataset:      projectID,
		readSettings: &readSettings{},
		databaseID:   databaseID,
		config:       &config,
	}, nil
}

func detectProjectID(ctx context.Context, opts ...option.ClientOption) (string, error) {
	creds, err := transport.Creds(ctx, opts...)
	if err != nil {
		return "", fmt.Errorf("fetching creds: %w", err)
	}
	if creds.ProjectID == "" {
		return "", errors.New("datastore: see the docs on DetectProjectID")
	}
	return creds.ProjectID, nil
}

var (
	// ErrInvalidEntityType is returned when functions like Get or Next are
	// passed a dst or src argument of invalid type.
	ErrInvalidEntityType = errors.New("datastore: invalid entity type")
	// ErrInvalidKey is returned when an invalid key is presented.
	ErrInvalidKey = errors.New("datastore: invalid key")
	// ErrNoSuchEntity is returned when no entity was found for a given key.
	ErrNoSuchEntity = errors.New("datastore: no such entity")
	// ErrDifferentKeyAndDstLength is returned when the length of dst and key are different.
	ErrDifferentKeyAndDstLength = errors.New("datastore: keys and dst slices have different length")
	// ErrDifferentKeyAndTransformsLength is returned when the length of transforms and keys are different.
	ErrDifferentKeyAndTransformsLength = errors.New("datastore: keys and transforms slices have different length")
)

type multiArgType int

const (
	multiArgTypeInvalid multiArgType = iota
	multiArgTypePropertyLoadSaver
	multiArgTypeStruct
	multiArgTypeStructPtr
	multiArgTypeInterface
)

// ErrFieldMismatch is returned when a stored property cannot be loaded into a
// destination struct field because of a type mismatch, overflow, or missing or
// unexported field. StructType is the type of the struct pointed to by dst.
//
// All other matching fields are still loaded into dst when ErrFieldMismatch is
// returned. If multiple fields mismatch, only one mismatch is reported (which
// field is reported is non-deterministic). In [Iterator.Next], the entity's key
// is still returned and the iterator advances normally. Use
// [WithIgnoreFieldMismatch] to ignore this error on [Client] reads.
type ErrFieldMismatch struct {
	StructType reflect.Type
	FieldName  string
	Reason     string
}

func (e *ErrFieldMismatch) Error() string {
	return fmt.Sprintf("datastore: cannot load field %q into a %q: %s",
		e.FieldName, e.StructType, e.Reason)
}

// GeoPoint represents a location as latitude/longitude in degrees.
type GeoPoint struct {
	Lat, Lng float64
}

// Valid returns whether a GeoPoint is within [-90, 90] latitude and [-180, 180] longitude.
func (g GeoPoint) Valid() bool {
	return -90 <= g.Lat && g.Lat <= 90 && -180 <= g.Lng && g.Lng <= 180
}

func keyToProto(k *Key) *pb.Key {
	if k == nil {
		return nil
	}

	var path []*pb.Key_PathElement
	for {
		el := &pb.Key_PathElement{Kind: k.Kind}
		if k.ID != 0 {
			el.IdType = &pb.Key_PathElement_Id{Id: k.ID}
		} else if k.Name != "" {
			el.IdType = &pb.Key_PathElement_Name{Name: k.Name}
		}
		path = append(path, el)
		if k.Parent == nil {
			break
		}
		k = k.Parent
	}

	// The path should be in order [grandparent, parent, child]
	// We did it backward above, so reverse back.
	for i := 0; i < len(path)/2; i++ {
		path[i], path[len(path)-i-1] = path[len(path)-i-1], path[i]
	}

	key := &pb.Key{Path: path}
	if k.Namespace != "" {
		key.PartitionId = &pb.PartitionId{
			NamespaceId: k.Namespace,
		}
	}
	return key
}

// protoToKey decodes a protocol buffer representation of a key into an
// equivalent *Key object. If the key is invalid, protoToKey will return the
// invalid key along with ErrInvalidKey.
func protoToKey(p *pb.Key) (*Key, error) {
	var key *Key
	var namespace string
	if partition := p.PartitionId; partition != nil {
		namespace = partition.NamespaceId
	}
	for _, el := range p.Path {
		key = &Key{
			Namespace: namespace,
			Kind:      el.Kind,
			ID:        el.GetId(),
			Name:      el.GetName(),
			Parent:    key,
		}
	}
	if !key.valid() { // Also detects key == nil.
		return key, ErrInvalidKey
	}
	return key, nil
}

// multiKeyToProto is a batch version of keyToProto.
func multiKeyToProto(keys []*Key) []*pb.Key {
	ret := make([]*pb.Key, len(keys))
	for i, k := range keys {
		ret[i] = keyToProto(k)
	}
	return ret
}

// multiProtoToKey is a batch version of protoToKey.
func multiProtoToKey(keys []*pb.Key) ([]*Key, error) {
	hasErr := false
	ret := make([]*Key, len(keys))
	err := make(MultiError, len(keys))
	for i, k := range keys {
		ret[i], err[i] = protoToKey(k)
		if err[i] != nil {
			hasErr = true
		}
	}
	if hasErr {
		return nil, err
	}
	return ret, nil
}

// multiValid is a batch version of Key.valid. It returns an error, not a
// []bool.
func multiValid(key []*Key) error {
	invalid := false
	for _, k := range key {
		if !k.valid() {
			invalid = true
			break
		}
	}
	if !invalid {
		return nil
	}
	err := make(MultiError, len(key))
	for i, k := range key {
		if !k.valid() {
			err[i] = ErrInvalidKey
		}
	}
	return err
}

// checkMultiArg checks that v has type []S, []*S, []I, or []P, for some struct
// type S, for some interface type I, or some non-interface non-pointer type P
// such that P or *P implements PropertyLoadSaver.
//
// It returns what category the slice's elements are, and the reflect.Type
// that represents S, I or P.
//
// As a special case, PropertyList is an invalid type for v.
func checkMultiArg(v reflect.Value) (m multiArgType, elemType reflect.Type) {
	// TODO(djd): multiArg is very confusing. Fold this logic into the
	// relevant Put/Get methods to make the logic less opaque.
	if v.Kind() != reflect.Slice {
		return multiArgTypeInvalid, nil
	}
	if v.Type() == typeOfPropertyList {
		return multiArgTypeInvalid, nil
	}
	elemType = v.Type().Elem()
	if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
		return multiArgTypePropertyLoadSaver, elemType
	}
	switch elemType.Kind() {
	case reflect.Struct:
		return multiArgTypeStruct, elemType
	case reflect.Interface:
		return multiArgTypeInterface, elemType
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			return multiArgTypeStructPtr, elemType
		}
	}
	return multiArgTypeInvalid, nil
}

// processFieldMismatchError ignore FieldMismatchErr if WithIgnoreFieldMismatch client option is provided by user
func (c *Client) processFieldMismatchError(err error) error {
	if c.config == nil || !c.config.ignoreFieldMismatchErrors {
		return err
	}
	return ignoreFieldMismatchErrs(err)
}

func ignoreFieldMismatchErrs(err error) error {
	if err == nil {
		return err
	}

	multiErr, isMultiErr := err.(MultiError)
	if isMultiErr {
		foundErr := false
		for i, e := range multiErr {
			multiErr[i] = ignoreFieldMismatchErr(e)
			if multiErr[i] != nil {
				foundErr = true
			}
		}
		if !foundErr {
			return nil
		}
		return multiErr
	}

	return ignoreFieldMismatchErr(err)
}

func ignoreFieldMismatchErr(err error) error {
	if err == nil {
		return err
	}
	_, isFieldMismatchErr := err.(*ErrFieldMismatch)
	if isFieldMismatchErr {
		return nil
	}
	return err
}

// Close closes the Client and releases its underlying gRPC connection pool.
// Call Close when done with the Client, and do not call it concurrently with
// other Client methods.
func (c *Client) Close() error {
	return c.connPool.Close()
}

// Get loads the entity stored for key into dst, which must be a struct pointer
// or implement [PropertyLoadSaver]. If there is no such entity for the key, Get
// returns [ErrNoSuchEntity].
//
// Unmatched struct fields in dst are not modified. For matching slice-typed
// fields (other than []byte), pre-populated elements are overwritten starting
// at index 0 and any trailing elements beyond the loaded array length are
// retained. Always pass a pointer to a fresh zero-valued struct on each Get
// call.
//
// *[ErrFieldMismatch] is returned only if dst is a struct pointer. Matching
// fields are still loaded into dst, and only one mismatch is reported if
// multiple exist.
func (c *Client) Get(ctx context.Context, key *Key, dst interface{}) (err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/datastore.Get")
	defer func() { trace.EndSpan(ctx, err) }()

	if dst == nil { // get catches nil interfaces; we need to catch nil ptr here
		return fmt.Errorf("%w: dst cannot be nil", ErrInvalidEntityType)
	}

	var opts *pb.ReadOptions
	if c.readSettings.readTimeExists() {
		opts = &pb.ReadOptions{
			ConsistencyType: &pb.ReadOptions_ReadTime{
				// Timestamp cannot be less than microseconds accuracy. See #6938
				ReadTime: &timestamppb.Timestamp{Seconds: c.readSettings.readTime.Unix()},
			},
		}
	}

	// Since opts does not contain Transaction option, 'get' call below will return nil
	// as transaction id which can be ignored
	_, err = c.get(ctx, []*Key{key}, []interface{}{dst}, opts)
	if me, ok := err.(MultiError); ok {
		return c.processFieldMismatchError(me[0])
	}
	return c.processFieldMismatchError(err)
}

// GetMulti is a batch version of [Client.Get].
//
// dst must be a slice value (not a pointer to a slice, unlike [Client.GetAll])
// pre-sized to len(keys), or [ErrDifferentKeyAndDstLength] is returned. Its type
// must be []S, []*S, []I, or []P for some struct type S, interface type I, or
// non-interface non-pointer type P such that P or *P implements
// [PropertyLoadSaver]. For []I, each element must be a valid dst for Get.
// As a special case, [PropertyList] is rejected as dst (use []PropertyList).
//
// Datastore allows at most 1,000 keys per lookup request, and GetMulti does not
// automatically chunk larger slices. Deferred keys in a lookup response are
// fetched automatically.
//
// Per-entity errors (such as [ErrNoSuchEntity] or *[ErrFieldMismatch]) are
// returned as a [MultiError] aligned 1:1 with keys, while whole-call RPC or
// context errors are returned directly. Because [MultiError] does not implement
// Unwrap, extract it with errors.As(err, &me) and inspect me[i].
func (c *Client) GetMulti(ctx context.Context, keys []*Key, dst interface{}) (err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/datastore.GetMulti")
	defer func() { trace.EndSpan(ctx, err) }()

	var opts *pb.ReadOptions
	if c.readSettings.readTimeExists() {
		opts = &pb.ReadOptions{
			ConsistencyType: &pb.ReadOptions_ReadTime{
				// Timestamp cannot be less than microseconds accuracy. See #6938
				ReadTime: &timestamppb.Timestamp{Seconds: c.readSettings.readTime.Unix()},
			},
		}
	}

	// Since opts does not contain Transaction option, 'get' call below will return nil
	// as transaction id which can be ignored
	_, err = c.get(ctx, keys, dst, opts)
	return c.processFieldMismatchError(err)
}

func (c *Client) get(ctx context.Context, keys []*Key, dst interface{}, opts *pb.ReadOptions) ([]byte, error) {
	v := reflect.ValueOf(dst)

	var multiArgType multiArgType

	// If kind is not of type slice, return error.
	if kind := v.Kind(); kind != reflect.Slice {
		return nil, fmt.Errorf("%w: dst: expected slice got %v", ErrInvalidEntityType, kind.String())
	}

	// if type is a type which implements PropertyList, return error
	if argType := v.Type(); argType == typeOfPropertyList {
		return nil, fmt.Errorf("%w: dst: cannot be PropertyListType", ErrInvalidEntityType)
	}

	elemType := v.Type().Elem()
	if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
		multiArgType = multiArgTypePropertyLoadSaver
	}

	switch elemType.Kind() {
	case reflect.Struct:
		multiArgType = multiArgTypeStruct
	case reflect.Interface:
		multiArgType = multiArgTypeInterface
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			multiArgType = multiArgTypeStructPtr
		}
	}

	dstLen := v.Len()

	if keysLen := len(keys); keysLen != dstLen {
		return nil, fmt.Errorf("%w: key length = %d, dst length = %d", ErrDifferentKeyAndDstLength, keysLen, v.Len())
	}

	if len(keys) == 0 {
		return nil, nil
	}

	// Go through keys, validate them, serialize then, and create a dict mapping them to their indices.
	// Equal keys are deduped.
	multiErr, any := make(MultiError, len(keys)), false
	keyMap := make(map[string][]int, len(keys))
	pbKeys := make([]*pb.Key, 0, len(keys))
	for i, k := range keys {
		if !k.valid() {
			multiErr[i] = ErrInvalidKey
			any = true
		} else if k.Incomplete() {
			multiErr[i] = fmt.Errorf("datastore: can't get the incomplete key: %v", k)
			any = true
		} else {
			ks := k.stringInternal()
			if _, ok := keyMap[ks]; !ok {
				pbKeys = append(pbKeys, keyToProto(k))
			}
			keyMap[ks] = append(keyMap[ks], i)
		}
	}
	if any {
		return nil, multiErr
	}
	req := &pb.LookupRequest{
		ProjectId:   c.dataset,
		DatabaseId:  c.databaseID,
		Keys:        pbKeys,
		ReadOptions: opts,
	}
	resp, err := c.client.Lookup(ctx, req)

	if err != nil {
		return nil, err
	}
	txnID := resp.Transaction
	found := resp.Found
	missing := resp.Missing
	// Upper bound 1000 iterations to prevent infinite loop. This matches the max
	// number of Entities you can request from Datastore.
	// Note that if ctx has a deadline, the deadline will probably
	// be hit before we reach 1000 iterations.
	for i := 0; len(resp.Deferred) > 0 && i < 1000; i++ {
		req.Keys = resp.Deferred
		resp, err = c.client.Lookup(ctx, req)
		if err != nil {
			return txnID, err
		}
		found = append(found, resp.Found...)
		missing = append(missing, resp.Missing...)
	}

	filled := 0
	for _, e := range found {
		k, err := protoToKey(e.Entity.Key)
		if err != nil {
			return txnID, errors.New("datastore: internal error: server returned an invalid key")
		}
		filled += len(keyMap[k.stringInternal()])
		for _, index := range keyMap[k.stringInternal()] {
			elem := v.Index(index)
			if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
				elem = elem.Addr()
			}
			if multiArgType == multiArgTypeStructPtr && elem.IsNil() {
				elem.Set(reflect.New(elem.Type().Elem()))
			}
			if err := loadEntityProto(elem.Interface(), e.Entity); err != nil {
				multiErr[index] = err
				any = true
			}
		}
	}
	for _, e := range missing {
		k, err := protoToKey(e.Entity.Key)
		if err != nil {
			return txnID, errors.New("datastore: internal error: server returned an invalid key")
		}
		filled += len(keyMap[k.stringInternal()])
		for _, index := range keyMap[k.stringInternal()] {
			multiErr[index] = ErrNoSuchEntity
		}
		any = true
	}

	if filled != len(keys) {
		return txnID, errors.New("datastore: internal error: server returned the wrong number of entities")
	}

	if any {
		return txnID, multiErr
	}
	return txnID, nil
}

// PutRequest represents a single entity's data along with any server-side
// transforms for [Client.PutWithOptions], [Client.PutMultiWithOptions], and
// their [Transaction] equivalents.
//
// PutRequest does not support a property mask, so the entire entity is
// overwritten with Entity before Transforms are applied. To apply transforms
// without overwriting existing properties, use [Client.Mutate] (or
// [Transaction.Mutate]) with [NewUpsert] or [NewUpdate], an empty
// [Mutation.WithPropertyMask](), and [Mutation.WithTransforms].
type PutRequest struct {
	// Key is the entity's key. It is required for all operations.
	Key *Key
	// Entity is the data to be saved. It must be a struct pointer or
	// implement [PropertyLoadSaver].
	Entity interface{}
	// Transforms is an optional slice of server-side transformations to apply
	// to the entity's properties. The transforms are applied atomically
	// by the server in the order they are provided in this slice.
	Transforms []PropertyTransform
}

// Put saves the entity src into the datastore with the given key. src must be
// a struct pointer or implement [PropertyLoadSaver]. Unexported struct fields
// are skipped.
//
// If key already exists, Put overwrites the entire entity (use [Client.Mutate]
// with [Mutation.WithPropertyMask] for partial updates). If key is incomplete,
// Put returns a newly generated complete key and does not mutate the input key.
//
// All struct fields are indexed by default. Strings and []byte values longer
// than 1,500 bytes must be tagged `datastore:",noindex"`, strings must be valid
// UTF-8, and an entity may have at most 20,000 indexed properties.
func (c *Client) Put(ctx context.Context, key *Key, src interface{}) (*Key, error) {
	return c.PutWithOptions(ctx, &PutRequest{Key: key, Entity: src})
}

// A PutOption is an optional argument to [Client.PutWithOptions] and [Client.PutMultiWithOptions].
type PutOption interface {
	isPutOption()
}

// PutWithOptions saves a single entity and applies any [PropertyTransform]
// operations in req.Transforms atomically on the server in slice order.
//
// If req.Key is incomplete, the returned key is a unique key generated by
// Datastore and the input req.Key is not mutated. Otherwise, PutWithOptions
// overwrites the entire entity with req.Entity before applying req.Transforms.
// To apply transforms without overwriting existing properties, use
// [Client.Mutate] with [NewUpsert] or [NewUpdate], an empty
// [Mutation.WithPropertyMask](), and [Mutation.WithTransforms].
func (c *Client) PutWithOptions(ctx context.Context, req *PutRequest, opts ...PutOption) (*Key, error) {
	k, err := c.PutMultiWithOptions(ctx, []*PutRequest{req})
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return k[0], nil
}

// PutMulti is a batch version of [Client.Put].
//
// src must be a slice value pre-sized to len(keys) satisfying the same
// conditions as dst in [Client.GetMulti] (not a pointer to a slice). Input
// incomplete keys are not mutated.
//
// Writes are committed in NON_TRANSACTIONAL mode (not atomic across entities).
// Requests are subject to Datastore's 10 MiB maximum API request size, and
// PutMulti does not automatically chunk larger batches. Duplicate keys in a
// single PutMulti call fail on the server. Per-entity validation errors return
// [MultiError], while whole-call RPC errors are returned directly.
func (c *Client) PutMulti(ctx context.Context, keys []*Key, src interface{}) (ret []*Key, err error) {
	// TODO(jba): rewrite in terms of Mutate.
	return c.putMultiInternal(ctx, keys, src, nil)
}

// PutMultiWithOptions is a batch version of [Client.PutWithOptions].
//
// Each [PutRequest] replaces its entire entity before applying its transforms
// (use [Client.Mutate] with an empty [Mutation.WithPropertyMask]() for
// transform-only updates). Writes are committed in NON_TRANSACTIONAL mode (not
// atomic across entities), subject to Datastore's 10 MiB maximum API request
// size with no automatic chunking. The returned keys match the order of reqs,
// and input incomplete keys are not mutated. Per-entity validation errors
// return [MultiError], while whole-call RPC errors are returned directly.
func (c *Client) PutMultiWithOptions(ctx context.Context, reqs []*PutRequest, opts ...PutOption) (ret []*Key, err error) {
	var keys []*Key
	var transforms [][]PropertyTransform
	var srcs []interface{}

	for _, req := range reqs {
		keys = append(keys, req.Key)
		transforms = append(transforms, req.Transforms)
		srcs = append(srcs, req.Entity)
	}
	return c.putMultiInternal(ctx, keys, srcs, transforms)
}

func (c *Client) putMultiInternal(ctx context.Context, keys []*Key, src interface{}, transforms [][]PropertyTransform) (ret []*Key, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/datastore.PutMulti")
	defer func() { trace.EndSpan(ctx, err) }()

	mutations, err := putMutations(keys, src, transforms)
	if err != nil {
		return nil, err
	}

	// Make the request.
	req := &pb.CommitRequest{
		ProjectId:  c.dataset,
		DatabaseId: c.databaseID,
		Mutations:  mutations,
		Mode:       pb.CommitRequest_NON_TRANSACTIONAL,
	}
	resp, err := c.client.Commit(ctx, req)
	if err != nil {
		return nil, err
	}

	// Copy any newly minted keys into the returned keys.
	ret = make([]*Key, len(keys))
	for i, key := range keys {
		if key.Incomplete() {
			// This key is in the mutation results.
			ret[i], err = protoToKey(resp.MutationResults[i].Key)
			if err != nil {
				return nil, errors.New("datastore: internal error: server returned an invalid key")
			}
		} else {
			ret[i] = key
		}
	}
	return ret, nil
}

func putMutations(keys []*Key, src interface{}, transforms [][]PropertyTransform) ([]*pb.Mutation, error) {
	v := reflect.ValueOf(src)
	var multiArgType multiArgType

	// If kind is not of type slice, return error.
	if kind := v.Kind(); kind != reflect.Slice {
		return nil, fmt.Errorf("%w: dst: expected slice got %v", ErrInvalidEntityType, kind.String())
	}

	// if type is a type which implements PropertyList, return error
	if argType := v.Type(); argType == typeOfPropertyList {
		return nil, fmt.Errorf("%w: dst: cannot be PropertyListType", ErrInvalidEntityType)
	}

	elemType := v.Type().Elem()
	if reflect.PtrTo(elemType).Implements(typeOfPropertyLoadSaver) {
		multiArgType = multiArgTypePropertyLoadSaver
	}

	switch elemType.Kind() {
	case reflect.Struct:
		multiArgType = multiArgTypeStruct
	case reflect.Interface:
		multiArgType = multiArgTypeInterface
	case reflect.Ptr:
		elemType = elemType.Elem()
		if elemType.Kind() == reflect.Struct {
			multiArgType = multiArgTypeStructPtr
		}
	}
	if multiArgType == multiArgTypeInvalid {
		return nil, fmt.Errorf("datastore: src has invalid type")
	}
	dstLen := v.Len()
	keysLen := len(keys)
	if keysLen != dstLen {
		return nil, fmt.Errorf("%w: key length = %d, dst length = %d", ErrDifferentKeyAndDstLength, keysLen, v.Len())
	}
	// Only validate transforms length if transforms were provided.
	transformsLen := len(transforms)
	if transforms != nil && keysLen != transformsLen {
		return nil, fmt.Errorf("%w: key length = %d, transforms length = %d", ErrDifferentKeyAndTransformsLength, keysLen, transformsLen)
	}

	if len(keys) == 0 {
		return nil, nil
	}
	if err := multiValid(keys); err != nil {
		return nil, err
	}
	mutations := make([]*pb.Mutation, 0, len(keys))
	multiErr := make(MultiError, len(keys))
	hasErr := false
	for i, k := range keys {
		elem := v.Index(i)
		// Two cases where we need to take the address:
		// 1) multiArgTypePropertyLoadSaver => &elem implements PLS
		// 2) multiArgTypeStruct => saveEntity needs *struct
		if multiArgType == multiArgTypePropertyLoadSaver || multiArgType == multiArgTypeStruct {
			elem = elem.Addr()
		}
		p, err := saveEntity(k, elem.Interface())
		if err != nil {
			multiErr[i] = err
			hasErr = true
		}

		var pbTransforms []*pb.PropertyTransform
		// Check if transforms were provided for this item.
		if transforms != nil && i < len(transforms) {
			for _, t := range transforms[i] {
				if t.err != nil {
					multiErr[i] = t.err
					hasErr = true
				}
				if t.pb != nil {
					pbTransforms = append(pbTransforms, t.pb)
				}
			}
		}
		var mut *pb.Mutation
		if k.Incomplete() {
			mut = &pb.Mutation{Operation: &pb.Mutation_Insert{Insert: p}, PropertyTransforms: pbTransforms}
		} else {
			mut = &pb.Mutation{Operation: &pb.Mutation_Upsert{Upsert: p}, PropertyTransforms: pbTransforms}
		}
		mutations = append(mutations, mut)
	}
	if hasErr {
		return nil, multiErr
	}
	return mutations, nil
}

// Delete deletes the entity for the given key. Deleting a nonexistent key
// succeeds without error.
func (c *Client) Delete(ctx context.Context, key *Key) error {
	err := c.DeleteMulti(ctx, []*Key{key})
	if me, ok := err.(MultiError); ok {
		return me[0]
	}
	return err
}

// DeleteMulti is a batch version of [Client.Delete]. Deleting nonexistent keys
// succeeds without error.
//
// Deletions are committed in NON_TRANSACTIONAL mode (not atomic across
// entities), subject to Datastore's 10 MiB maximum API request size with no
// automatic chunking. Per-key validation errors return [MultiError], while
// whole-call RPC errors are returned directly.
func (c *Client) DeleteMulti(ctx context.Context, keys []*Key) (err error) {
	// TODO(jba): rewrite in terms of Mutate.
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/datastore.DeleteMulti")
	defer func() { trace.EndSpan(ctx, err) }()

	mutations, err := deleteMutations(keys)
	if err != nil {
		return err
	}

	req := &pb.CommitRequest{
		ProjectId:  c.dataset,
		DatabaseId: c.databaseID,
		Mutations:  mutations,
		Mode:       pb.CommitRequest_NON_TRANSACTIONAL,
	}
	_, err = c.client.Commit(ctx, req)
	return err
}

func deleteMutations(keys []*Key) ([]*pb.Mutation, error) {
	mutations := make([]*pb.Mutation, 0, len(keys))
	set := make(map[string]bool, len(keys))
	multiErr := make(MultiError, len(keys))
	hasErr := false
	for i, k := range keys {
		if !k.valid() {
			multiErr[i] = ErrInvalidKey
			hasErr = true
		} else if k.Incomplete() {
			multiErr[i] = fmt.Errorf("datastore: can't delete the incomplete key: %v", k)
			hasErr = true
		} else {
			ks := k.stringInternal()
			if !set[ks] {
				mutations = append(mutations, &pb.Mutation{
					Operation: &pb.Mutation_Delete{Delete: keyToProto(k)},
				})
			}
			set[ks] = true
		}
	}
	if hasErr {
		return nil, multiErr
	}
	return mutations, nil
}

// Mutate applies one or more mutations in NON_TRANSACTIONAL mode (not atomic
// across entities). If you need atomicity, use [Transaction.Mutate]. Requests
// are subject to Datastore's 10 MiB maximum API request size, and Mutate does
// not automatically chunk larger batches.
//
// Mutate returns the keys of muts in the same order without mutating incomplete
// input keys. If any mutation has a client-side validation error, Mutate returns
// a [MultiError] even when only one mutation was passed. Whole-call RPC errors
// (such as gRPC codes.NotFound for [NewUpdate] or codes.AlreadyExists for
// [NewInsert]) are returned directly.
func (c *Client) Mutate(ctx context.Context, muts ...*Mutation) (ret []*Key, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/datastore.Mutate")
	defer func() { trace.EndSpan(ctx, err) }()

	pmuts, err := mutationProtos(muts)
	if err != nil {
		return nil, err
	}
	req := &pb.CommitRequest{
		ProjectId:  c.dataset,
		DatabaseId: c.databaseID,
		Mutations:  pmuts,
		Mode:       pb.CommitRequest_NON_TRANSACTIONAL,
	}
	resp, err := c.client.Commit(ctx, req)
	if err != nil {
		return nil, err
	}
	// Copy any newly minted keys into the returned keys.
	ret = make([]*Key, len(muts))
	for i, mut := range muts {
		if mut.key.Incomplete() {
			// This key is in the mutation results.
			ret[i], err = protoToKey(resp.MutationResults[i].Key)
			if err != nil {
				return nil, errors.New("datastore: internal error: server returned an invalid key")
			}
		} else {
			ret[i] = mut.key
		}
	}
	return ret, nil
}

// ReadTime specifies a snapshot timestamp of the database to read entities from.
// Pass ReadTime(time.Time{}) to [Client.WithReadOptions] to clear a previously
// configured read time.
//
// Datastore requires microsecond precision (truncate time.Now() with
// t.Truncate(time.Microsecond)) within the past 1 hour by default, or
// whole-minute timestamps within the past 7 days when point-in-time recovery
// (PITR) is enabled. Note that [Client.Get] and [Client.GetMulti] truncate a
// client-level ReadTime to whole seconds.
func ReadTime(t time.Time) ReadOption {
	return docReadTime(t)
}

type docReadTime time.Time

func (drt docReadTime) apply(rs *readSettings) {
	rs.readTime = time.Time(drt)
}

// ReadOption provides instructions for how to access entities in the database.
// Currently, only [ReadTime] is supported.
type ReadOption interface {
	apply(*readSettings)
}

type readSettings struct {
	readTime time.Time
}

func (rs *readSettings) readTimeExists() bool {
	return rs != nil && !rs.readTime.IsZero()
}

// WithReadOptions configures read constraints for accessing entities from the
// database, such as reading at a point-in-time snapshot via [ReadTime].
//
// WithReadOptions mutates the receiver [Client] in place without locking and is
// not safe to call concurrently with reads on a shared Client. The configured
// options apply to all subsequent reads until cleared with
// ReadTime(time.Time{}). Client-level [ReadTime] is incompatible with
// transactions and [Query.EventualConsistency].
func (c *Client) WithReadOptions(ro ...ReadOption) *Client {
	for _, r := range ro {
		r.apply(c.readSettings)
	}
	return c
}
