# Juttle InfluxDB Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-influx-adapter.svg?branch=master)](https://travis-ci.org/juttle/juttle-influx-adapter)

[InfluxDB](https://github.com/influxdata/influxdb) adapter for the [Juttle data flow
language](https://github.com/juttle/juttle), with read & write support.

Currently supports InfluxDB 0.9 and 0.10.

## Examples

Read entries from the `cpu` measurement where the `host` tag is `www123`:

```juttle
read influx -db 'test' name='cpu' host='www123' | view text
```

Perform an equivalent query using the -raw option:

```juttle
read influx -db 'test' -raw "SELECT * FROM cpu where host='www123' | view text
```

Write a single point into the `cpu` measurement:

```juttle
emit -points [{ value: 0.01, host: 'www123', name: 'cpu' }] | write influx -db 'test'
```

## Installation

Like Juttle itself, the adapter is installed as a npm package. Both Juttle and
the adapter need to be installed side-by-side:

```bash
$ npm install juttle
$ npm install juttle-influx-adapter
```

## Configuration

The adapter needs to be registered and configured so that it can be used from
within Juttle. To do so, add the following to your `~/.juttle/config.json` file:

```json
{
    "adapters": {
        "influx": {
            "url": "http://localhost:8086/"
        }
    }
}
```

The URL in the `url` key should point to the API url of your InfluxDB instance.

### Authentication

Influx adapter supports HTTP basic authentication. To use it, modify the `url`
key to include the username and password:

```json
{
    "adapters": {
        "influx": {
            "url": "http://username:password@localhost:8086/"
        }
    }
}
```

## Usage

### Read options

When reading data, most of the InfluxQL SELECT syntax is expressible through Juttle [filter expressions](http://juttle.github.io/juttle/concepts/filtering/).

Alternatively, raw queries are also available as a fallback.

Name | Type | Required | Description
-----|------|----------|-------------
`db`   | string | yes | database to use
`raw`  | string | no  | send a raw InfluxQL query to InfluxDB
`fields` | string | no | fields to select from the measurement (default: all)
`nameField` | string | no | if specified, measurement name will be saved in a point field with this name (default: 'name')
`from` | moment | no | select points after this time (inclusive)
`to`   | moment | no | select points before this time (exclusive)

The filter expressions can be placed after the above options in `read influx`. Supported filters are:

- `fieldname = value` for both tags and field values (also, `!=`, and `<`/`>` for numbers)
- `fieldname ~ '*glob*'` wildcard matching (also, `!~`) for string fields
- `fieldname =~ /regex/` regex matching (also, `!~`) for string fields
- `fieldname in [v1, v2]` array inclusion
- combining filter expressions with `AND`, `OR`, `NOT`

Influx adapter does not support full text search, or [nullness checks](https://github.com/juttle/juttle-influx-adapter/issues/73).

### Write options

Name | Type | Required | Description
-----|------|----------|-------------
`db`   | string | yes | database to use
`intFields` | array | no | lists fields to be stored as integers instead of floats (default: none)
`valFields` | array | no | lists fields to be stored as values instead of tags (default: all non-string fields)
`nameField` | string | no | points will be checked for this field and its value will be used as the measurement name (default: 'name')

Note: when storing points, the following conventions are used:

1. All fields whose values are strings are treated as
   [tags](https://influxdb.com/docs/v0.9/concepts/key_concepts.html#tag-key).
   and all fields with numeric types are treated as fields.
   You can override this behavior using the `valueFields` option. For
   example, `... | write influx -valueFields 'foo', 'bar'` will treat `foo` and
   `bar` as fields, not tags.

2. InfluxDB distinguishes between integers and floating point numeric types. By
   default, the adapter stores all numeric fields as floats. This can be changed
   by enumerating integer fields via `intFields` option.

### Optimizations

Whenever the influx adapter can shape the entire Juttle flowgraph or its portion into an InfluxDB query, it will do so, sending the execution to InfluxDB, so only the matching data will come back into Juttle runtime. The portion of the program expressed in `read influx` is always executed as an InfluxDB query; the downstream Juttle processors may be optimized as well.

List of optimized operations:

- any filter expression as part of `read influx` (note: `read influx | filter ...` is not optimized)
- `head` or `tail`

Other operations such as [`reduce`](https://github.com/juttle/juttle-influx-adapter/issues/79) and [`sort`](https://github.com/juttle/juttle-influx-adapter/issues/78) are not currently optimized.

In case of unexpected behavior with optimized reads, add `-optimize false` option to `read influx` to disable optimizations, and kindly report the problem as a GitHub issue.

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
