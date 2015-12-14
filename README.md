# InfluxDB Backend for Juttle

[![Build Status](https://travis-ci.com/juttle/juttle-influx-backend.svg?token=C2AjzxBQoVUrmWXFQb7w)](https://travis-ci.com/juttle/juttle-influx-backend)

## About

InfluxDB backend for the [Juttle data flow
language](https://github.com/juttle/juttle), with read & write support.

Large part of InfluxQL SELECT syntax is accessible through options and raw mode
is also available as a fallback.

Currently, only InfluxDB 0.9.0 and newer, JSON (reading) and line protocol
over HTTP (writing) is supported.

### Examples:

Display all entries from the `cpu` measurement:

```bash
$ juttle -e 'read influxdb -db "test" -measurements "cpu" | view logger'
```

Write a single point into the `cpu` measurement:

```bash
$ juttle -e 'emit -points [{ "value": 0.01 }] | write influxdb -db "test" -measurement "cpu"
```

## Installation

The backend needs to be installed in the same directory as [juttle package](https://www.npmjs.com/package/juttle):

1. Create an empty directory

```bash
$ mkdir -p my_project
$ cd my_project
```

2. Install juttle package and the backend:

```bash
$ npm install juttle
$ npm install juttle-influx-backend
```

## Configuration

The backend needs to be registered and configured, so that it can be used from
within Juttle. To do so, add the following to your `~/.juttle/config.json` file:

```
{
    "backends": {
        "influxdb-backend": {
            "url": "http://localhost:8086/"
        }
    }
}
```

The URL points to the API url of your InfluxDB database.

## Usage

When storing points, following conventions are used:

1. All keys, whose values are strings, are treated as
   [tags](https://influxdb.com/docs/v0.9/concepts/key_concepts.html#tag-key).
   You can always override this behavior using `valueFields` option:
   `...|writex influxdb -valueFields "foo","bar"` will treat `foo` and `bar`
   as fields, not tags.

2. All numeric fields are stored as floats. This can be changed by enumerating
   the integer fields via `intFields` option.

For more complex examples, please take a look at Juttle scripts in the [examples]().

### Read options

Name | Type | Required | Description
-----|------|----------|-------------
raw  | string | no  | send a raw InfluxQL query to InfluxDB
db   | string | yes | database to use
mesurements | array | no | measurements to query, defaults to `/.*/`, i.e. all measurements
offset | integer| no | record offset
limit  | integer | no | query limit, defaults to 10000 records
measurementField | string | no | if specified, measurement name will be added to all points under this key
from | Juttle moment | no | time of the first point selected
to   | Juttle moment | no | time of the last point selected

### Write options

Name | Type | Required | Description
-----|------|----------|-------------
db   | string | yes | database to use
intFields | array | no | these fields will be stored as integers, instead of floats (default: none)
valFields | array | no | strings in these fields won't be stored as tags, but values
measurement | string | yes | measurement to use (required, optional if points contain measurement field, specified by measurementField option)
measurementField | string | no | points will be checked for this field and its value will be used as the measurement name

## Contributing

Want to contribute? Awesome! We encourage everyone to contribute to Juttle and
its backends. To contribute to InfluxDB backend, don't hesitate to file an
issue or open a pull request.
