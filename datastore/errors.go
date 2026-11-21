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

// This file provides error functions for common API failure modes.

package datastore

import (
	"fmt"
)

// MultiError is returned by batch operations ([Client.GetMulti], [Client.PutMulti],
// [Client.DeleteMulti], [Client.Mutate], and their [Transaction] equivalents) when
// per-entity errors occur. Errors are in one-to-one correspondence with the input
// elements, with nil entries for elements that succeeded. Whole-call RPC or context
// errors are returned directly rather than as MultiError.
//
// MultiError does not implement Unwrap, so errors.Is and errors.As do not match
// element errors such as [ErrNoSuchEntity] or *[ErrFieldMismatch]. Extract
// MultiError with errors.As(err, &me) and inspect each me[i] individually.
type MultiError []error

func (m MultiError) Error() string {
	s, n := "", 0
	for _, e := range m {
		if e != nil {
			if n == 0 {
				s = e.Error()
			}
			n++
		}
	}
	switch n {
	case 0:
		return "(0 errors)"
	case 1:
		return s
	case 2:
		return s + " (and 1 other error)"
	}
	return fmt.Sprintf("%s (and %d other errors)", s, n-1)
}
