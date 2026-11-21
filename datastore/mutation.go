// Copyright 2018 Google LLC
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
	"errors"
	"fmt"

	pb "cloud.google.com/go/datastore/apiv1/datastorepb"
)

// A Mutation represents a change to a Datastore entity.
// Validation errors from constructors and builder methods are deferred and
// returned in a [MultiError] when applied via [Client.Mutate] or [Transaction.Mutate].
type Mutation struct {
	key *Key // needed for transaction PendingKeys and to dedup deletions
	mut *pb.Mutation

	// err is set to a Datastore or gRPC error, if Mutation is not valid
	// (see https://pkg.go.dev/google.golang.org/grpc/codes).
	err error
}

func (m *Mutation) isDelete() bool {
	_, ok := m.mut.Operation.(*pb.Mutation_Delete)
	return ok
}

// WithTransforms adds one or more server-side [PropertyTransform] operations to
// the mutation. It can be called multiple times, preserving the order of calls
// and the order of transforms within each call. Transforms cannot be applied to
// a [NewDelete] mutation.
//
// Transforms are applied after the insert, upsert, or update operation, even if
// a transformed property is excluded from [Mutation.WithPropertyMask]. By
// default, [NewUpsert] and [NewUpdate] replace the entire entity before applying
// transforms. To perform a transform-only update without overwriting existing
// properties (for example, incrementing a counter), call WithPropertyMask() with
// no paths together with WithTransforms.
//
// Any invalid or uninitialized [PropertyTransform] is recorded on the mutation
// (reporting an uninitialized-transform error, whereas [Client.PutWithOptions]
// surfaces the underlying constructor error) and returned in a [MultiError]
// when applied via [Client.Mutate] or [Transaction.Mutate].
func (m *Mutation) WithTransforms(transforms ...PropertyTransform) *Mutation {
	if m.err != nil {
		return m
	}
	if m.mut == nil {
		m.err = errors.New("datastore: WithTransforms called on uninitialized mutation")
		return m
	}
	if m.isDelete() {
		m.err = errors.New("datastore: property transforms cannot be applied to a delete mutation")
		return m
	}

	for _, transform := range transforms {
		if transform.pb == nil {
			m.err = errors.New("datastore: WithTransforms called with an uninitialized PropertyTransform")
			return m
		}
		m.mut.PropertyTransforms = append(m.mut.PropertyTransforms, transform.pb)
	}
	return m
}

// WithPropertyMask specifies which properties of the entity should be updated.
// It is only valid on [NewUpdate] and [NewUpsert] mutations (calling it on
// [NewInsert] or [NewDelete] records an error returned by [Client.Mutate] or
// [Transaction.Mutate]).
//
// If paths is empty, no properties are overwritten. Combine an empty
// WithPropertyMask() call with [Mutation.WithTransforms] to apply server-side
// transforms without overwriting existing properties. If WithPropertyMask is not
// called, [NewUpsert] and [NewUpdate] replace the entire entity.
func (m *Mutation) WithPropertyMask(paths ...string) *Mutation {
	if m.err != nil {
		return m
	}
	if m.mut == nil {
		m.err = errors.New("datastore: WithPropertyMask called on uninitialized mutation")
		return m
	}
	if _, isInsert := m.mut.GetOperation().(*pb.Mutation_Insert); isInsert || m.isDelete() {
		m.err = errors.New("datastore: property mask can only be applied to update or upsert mutations")
		return m
	}

	// We permit an empty slice for paths, which means "update no properties".
	// However, we must ensure m.mut.PropertyMask is not nil in that case.
	if m.mut.PropertyMask == nil {
		m.mut.PropertyMask = &pb.PropertyMask{}
	}
	m.mut.PropertyMask.Paths = append(m.mut.PropertyMask.Paths, paths...)
	return m
}

// NewInsert creates a [Mutation] that saves src under key k only if k does not
// already exist. If k already exists, committing the mutation fails with a gRPC
// codes.AlreadyExists error. If k is incomplete, k is not mutated (use the
// returned key from [Client.Mutate] or [Commit.Key]). See [Client.Put] for valid
// values of src.
func NewInsert(k *Key, src interface{}) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	p, err := saveEntity(k, src)
	if err != nil {
		return &Mutation{err: err}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Insert{Insert: p}},
	}
}

// NewUpsert creates a [Mutation] that saves src under key k whether or not k
// exists. If k is incomplete, k is not mutated (use the returned key from
// [Client.Mutate] or [Commit.Key]). See [Client.Put] for valid values of src.
//
// By default, NewUpsert replaces the entire entity. Call [Mutation.WithPropertyMask]
// for partial updates, or call WithPropertyMask() with no paths together with
// [Mutation.WithTransforms] for transform-only updates.
func NewUpsert(k *Key, src interface{}) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	p, err := saveEntity(k, src)
	if err != nil {
		return &Mutation{err: err}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Upsert{Upsert: p}},
	}
}

// NewUpdate creates a [Mutation] that replaces the existing entity with complete
// key k. If k does not exist, committing the mutation fails with a gRPC
// codes.NotFound error (not [ErrNoSuchEntity]). Call [Mutation.WithPropertyMask]
// for partial updates or transform-only updates. See [Client.Put] for valid
// values of src.
func NewUpdate(k *Key, src interface{}) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	if k.Incomplete() {
		return &Mutation{err: fmt.Errorf("datastore: can't update the incomplete key: %v", k)}
	}
	p, err := saveEntity(k, src)
	if err != nil {
		return &Mutation{err: err}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Update{Update: p}},
	}
}

// NewDelete creates a [Mutation] that deletes the entity with complete key k.
// Deleting a nonexistent key succeeds without error.
func NewDelete(k *Key) *Mutation {
	if !k.valid() {
		return &Mutation{err: ErrInvalidKey}
	}
	if k.Incomplete() {
		return &Mutation{err: fmt.Errorf("datastore: can't delete the incomplete key: %v", k)}
	}
	return &Mutation{
		key: k,
		mut: &pb.Mutation{Operation: &pb.Mutation_Delete{Delete: keyToProto(k)}},
	}
}

func mutationProtos(muts []*Mutation) ([]*pb.Mutation, error) {
	// If any of the mutations have errors, collect and return them.
	var merr MultiError
	for i, m := range muts {
		if m.err != nil {
			if merr == nil {
				merr = make(MultiError, len(muts))
			}
			merr[i] = m.err
		}
	}
	if merr != nil {
		return nil, merr
	}

	var protos []*pb.Mutation
	// Collect protos. Remove duplicate deletions (see deleteMutations).
	seen := map[string]bool{}
	for _, m := range muts {
		if m.isDelete() {
			ks := m.key.stringInternal()
			if seen[ks] {
				continue
			}
			seen[ks] = true
		}
		protos = append(protos, m.mut)
	}
	return protos, nil
}
