# Cloud Datastore API

[![Go Reference](https://pkg.go.dev/badge/cloud.google.com/go/datastore.svg)](https://pkg.go.dev/cloud.google.com/go/datastore)

Go Client Library for Cloud Datastore API.

## Install

```bash
go get cloud.google.com/go/datastore
```

## Stability

The stability of this module is indicated by SemVer.

However, a `v1+` module may have breaking changes in two scenarios:

* Packages with `alpha` or `beta` in the import path
* The GoDoc has an explicit stability disclaimer (for example, for an experimental feature).

### Which package to use?

* `cloud.google.com/go/datastore`: Handwritten Go client library for Cloud
  Datastore (and Firestore in Datastore mode) data-plane operations such as
  reading, writing, querying, and transactions.
* `cloud.google.com/go/datastore/apiv1/datastorepb`: Generated Protocol Buffer
  message types only (there is no generated `datastore/apiv1` Go client). Import
  this package when unpacking `*datastorepb.Value` entries from
  `AggregationResult`.
* `cloud.google.com/go/datastore/admin/apiv1`: Generated Go client for Cloud
  Datastore Admin operations such as exports, imports, and composite indexes.
* `cloud.google.com/go/firestore`: Use this client library instead if your
  database runs in Firestore Native mode rather than Datastore mode (see
  [Choosing between Native mode and Datastore mode](https://docs.cloud.google.com/datastore/docs/firestore-or-datastore)).

## Google Cloud Samples

To browse ready to use code samples check [Google Cloud Samples](https://docs.cloud.google.com/docs/samples?l=go).

## Go Version Support

See the [Go Versions Supported](https://github.com/googleapis/google-cloud-go#go-versions-supported)
section in the root directory's README.

## Authentication

See the [Authentication](https://github.com/googleapis/google-cloud-go#authentication)
section in the root directory's README.

## Contributing

Contributions are welcome. Please, see the [CONTRIBUTING](https://github.com/googleapis/google-cloud-go/blob/main/CONTRIBUTING.md)
document for details.

Please note that this project is released with a Contributor Code of Conduct.
By participating in this project you agree to abide by its terms. See
[Contributor Code of Conduct](https://github.com/googleapis/google-cloud-go/blob/main/CONTRIBUTING.md#contributor-code-of-conduct)
for more information.
