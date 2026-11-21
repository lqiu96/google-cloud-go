// Copyright 2019 Google LLC
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
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/internal/optional"
	"cloud.google.com/go/internal/trace"
	bq "google.golang.org/api/bigquery/v2"
)

// Model represents a reference to a BigQuery ML model.
// Creating a handle via [Dataset.Model] does not perform a network call.
// Models have no Create method and are created only via BigQuery SQL queries
// (such as CREATE MODEL ... AS ...).
//
// For more information about BigQuery ML, see
// https://docs.cloud.google.com/bigquery/docs/bqml-introduction.
type Model struct {
	ProjectID string
	DatasetID string
	// ModelID must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_).
	// The maximum length is 1,024 characters.
	ModelID string

	c *Client
}

// Identifier returns the ID of the model in the requested format.
// Only [LegacySQLID] and [StandardSQLID] are supported.
//
// For [StandardSQLID], the entire project.dataset.model identifier is
// backtick-quoted if any component contains a dash (-) character.
func (m *Model) Identifier(f IdentifierFormat) (string, error) {
	switch f {
	case LegacySQLID:
		return fmt.Sprintf("%s:%s.%s", m.ProjectID, m.DatasetID, m.ModelID), nil
	case StandardSQLID:
		// Per https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create#model_name
		// we quote the entire identifier.
		out := fmt.Sprintf("%s.%s.%s", m.ProjectID, m.DatasetID, m.ModelID)
		if strings.Contains(out, "-") {
			out = fmt.Sprintf("`%s`", out)
		}
		return out, nil
	default:
		return "", ErrUnknownIdentifierFormat
	}
}

// FullyQualifiedName returns the ID of the model in Legacy SQL
// projectID:datasetID.modelID format (unlike [Routine.FullyQualifiedName],
// which uses Standard SQL format).
func (m *Model) FullyQualifiedName() string {
	s, _ := m.Identifier(LegacySQLID)
	return s
}

func (m *Model) toBQ() *bq.ModelReference {
	return &bq.ModelReference{
		ProjectId: m.ProjectID,
		DatasetId: m.DatasetID,
		ModelId:   m.ModelID,
	}
}

// Metadata fetches the metadata for a model, which includes ML training statistics.
func (m *Model) Metadata(ctx context.Context) (mm *ModelMetadata, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/bigquery.Model.Metadata")
	defer func() { trace.EndSpan(ctx, err) }()

	ctx = setModelTraceMetadata(ctx, m.ProjectID, m.DatasetID, m.ModelID)
	req := m.c.bqs.Models.Get(m.ProjectID, m.DatasetID, m.ModelID).Context(ctx)
	setClientHeader(req.Header())
	var model *bq.Model
	err = runWithRetry(ctx, func() (err error) {
		sCtx := trace.StartSpan(ctx, "bigquery.models.get")
		model, err = req.Do()
		trace.EndSpan(sCtx, err)
		return err
	})
	if err != nil {
		return nil, err
	}
	return bqToModelMetadata(model), nil
}

// Update modifies mutable fields in an ML model using HTTP PATCH.
// Only non-nil fields in mm are updated. Pass etag from a prior [Model.Metadata]
// call for optimistic concurrency control (If-Match, failing with HTTP 412 on
// concurrent writes), or pass "" for a blind write.
func (m *Model) Update(ctx context.Context, mm ModelMetadataToUpdate, etag string) (md *ModelMetadata, err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/bigquery.Model.Update")
	defer func() { trace.EndSpan(ctx, err) }()

	bqm, err := mm.toBQ()
	if err != nil {
		return nil, err
	}
	ctx = setModelTraceMetadata(ctx, m.ProjectID, m.DatasetID, m.ModelID)
	call := m.c.bqs.Models.Patch(m.ProjectID, m.DatasetID, m.ModelID, bqm).Context(ctx)
	setClientHeader(call.Header())
	if etag != "" {
		call.Header().Set("If-Match", etag)
	}
	var res *bq.Model
	if err := runWithRetry(ctx, func() (err error) {
		sCtx := trace.StartSpan(ctx, "bigquery.models.patch")
		res, err = call.Do()
		trace.EndSpan(sCtx, err)
		return err
	}); err != nil {
		return nil, err
	}
	return bqToModelMetadata(res), nil
}

// Delete deletes an ML model.
func (m *Model) Delete(ctx context.Context) (err error) {
	ctx = trace.StartSpan(ctx, "cloud.google.com/go/bigquery.Model.Delete")
	defer func() { trace.EndSpan(ctx, err) }()

	ctx = setModelTraceMetadata(ctx, m.ProjectID, m.DatasetID, m.ModelID)
	req := m.c.bqs.Models.Delete(m.ProjectID, m.DatasetID, m.ModelID).Context(ctx)
	setClientHeader(req.Header())
	return req.Do()
}

// ModelMetadata represents information about a BigQuery ML model.
type ModelMetadata struct {
	// The user-friendly description of the model.
	Description string

	// The user-friendly name of the model.
	Name string

	// The type of the model.  Possible values include:
	// "LINEAR_REGRESSION" - a linear regression model
	// "LOGISTIC_REGRESSION" - a logistic regression model
	// "KMEANS" - a k-means clustering model
	Type string

	// The creation time of the model.
	CreationTime time.Time

	// The last modified time of the model.
	LastModifiedTime time.Time

	// The expiration time of the model.
	ExpirationTime time.Time

	// The geographic location where the model resides.  This value is
	// inherited from the encapsulating dataset.
	Location string

	// Custom encryption configuration (e.g., Cloud KMS keys).
	EncryptionConfig *EncryptionConfig

	// The input feature columns used to train the model.
	featureColumns []*bq.StandardSqlField

	// The label columns used to train the model.  Output
	// from the model will have a "predicted_" prefix for these columns.
	labelColumns []*bq.StandardSqlField

	// Information for all training runs, ordered by increasing start times.
	trainingRuns []*bq.TrainingRun

	Labels map[string]string

	// ETag is the ETag obtained when reading metadata. Pass it to Model.Update
	// to ensure that the metadata hasn't changed since it was read.
	ETag string
}

// TrainingRun represents information about a single training run for a BigQuery ML model.
// Experimental:  This information may be modified or removed in future versions of this package.
type TrainingRun bq.TrainingRun

// RawTrainingRuns exposes the underlying training run stats for a model using types from
// "google.golang.org/api/bigquery/v2", which are subject to change without warning.
// It is EXPERIMENTAL and subject to change or removal without notice.
func (mm *ModelMetadata) RawTrainingRuns() []*TrainingRun {
	if mm.trainingRuns == nil {
		return nil
	}
	var runs []*TrainingRun

	for _, v := range mm.trainingRuns {
		r := TrainingRun(*v)
		runs = append(runs, &r)
	}
	return runs
}

// RawLabelColumns exposes the label columns used to train an ML model as
// [StandardSQLField] values defined in this package.
// It is EXPERIMENTAL and subject to change or removal without notice.
func (mm *ModelMetadata) RawLabelColumns() ([]*StandardSQLField, error) {
	return bqToModelCols(mm.labelColumns)
}

// RawFeatureColumns exposes the feature columns used to train an ML model as
// [StandardSQLField] values defined in this package.
// It is EXPERIMENTAL and subject to change or removal without notice.
func (mm *ModelMetadata) RawFeatureColumns() ([]*StandardSQLField, error) {
	return bqToModelCols(mm.featureColumns)
}

func bqToModelCols(s []*bq.StandardSqlField) ([]*StandardSQLField, error) {
	if s == nil {
		return nil, nil
	}
	var cols []*StandardSQLField
	for _, v := range s {
		c, err := bqToStandardSQLField(v)
		if err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, nil
}

func bqToModelMetadata(m *bq.Model) *ModelMetadata {
	return &ModelMetadata{
		Description:      m.Description,
		Name:             m.FriendlyName,
		Type:             m.ModelType,
		Location:         m.Location,
		Labels:           m.Labels,
		ExpirationTime:   unixMillisToTime(m.ExpirationTime),
		CreationTime:     unixMillisToTime(m.CreationTime),
		LastModifiedTime: unixMillisToTime(m.LastModifiedTime),
		EncryptionConfig: bqToEncryptionConfig(m.EncryptionConfiguration),
		featureColumns:   m.FeatureColumns,
		labelColumns:     m.LabelColumns,
		trainingRuns:     m.TrainingRuns,
		ETag:             m.Etag,
	}
}

// ModelMetadataToUpdate is used when updating an ML model's metadata via [Model.Update].
// Only non-nil fields are updated.
//
// Fields typed as optional.String accept plain string values (not *string) and
// panic at runtime if given another type. Use SetLabel and DeleteLabel to add,
// modify, or remove labels.
type ModelMetadataToUpdate struct {
	// The user-friendly description of this model.
	Description optional.String

	// The user-friendly name of this model.
	Name optional.String

	// The time when this model expires. To remove a model's expiration,
	// set ExpirationTime to [NeverExpire]. The zero value is ignored.
	// Valid expiration times are between years 1678 and 2262.
	ExpirationTime time.Time

	// The model's encryption configuration.
	EncryptionConfig *EncryptionConfig

	labelUpdater
}

func (mm *ModelMetadataToUpdate) toBQ() (*bq.Model, error) {
	m := &bq.Model{}
	forceSend := func(field string) {
		m.ForceSendFields = append(m.ForceSendFields, field)
	}

	if mm.Description != nil {
		m.Description = optional.ToString(mm.Description)
		forceSend("Description")
	}

	if mm.Name != nil {
		m.FriendlyName = optional.ToString(mm.Name)
		forceSend("FriendlyName")
	}

	if mm.EncryptionConfig != nil {
		m.EncryptionConfiguration = mm.EncryptionConfig.toBQ()
	}

	if !validExpiration(mm.ExpirationTime) {
		return nil, invalidTimeError(mm.ExpirationTime)
	}
	if mm.ExpirationTime == NeverExpire {
		m.NullFields = append(m.NullFields, "ExpirationTime")
	} else if !mm.ExpirationTime.IsZero() {
		m.ExpirationTime = mm.ExpirationTime.UnixNano() / 1e6
		forceSend("ExpirationTime")
	}
	labels, forces, nulls := mm.update()
	m.Labels = labels
	m.ForceSendFields = append(m.ForceSendFields, forces...)
	m.NullFields = append(m.NullFields, nulls...)
	return m, nil
}
