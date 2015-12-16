# Juttle InfluxDB Adapter

[![Build Status](https://travis-ci.com/juttle/juttle-influx-adapter.svg?token=C2AjzxBQoVUrmWXFQb7w)](https://travis-ci.com/juttle/juttle-influx-adapter)

InfluxDB adapter for the [Juttle data flow
language](https://github.com/juttle/juttle), with read & write support.

Currently supports InfluxDB 0.9.

## Examples

Read entries from the `cpu` measurement where the `host` tag is `www123` and the default limit of 1000:

```juttle
read influx -db 'test' -measurements 'cpu' host='www123' | view logger
```

Perform an equivalent query using the -raw option:

```juttle
read influx -db 'test' -raw "SELECT * FROM cpu where host='www123 LIMIT 1000' | view logger
```

Write a single point into the `cpu` measurement:

```juttle
emit -points [{ value: 0.01, host: 'www123' }] | write influx -db 'test' -measurement 'cpu'
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
        "juttle-influx-adapter": {
            "url": "http://localhost:8086/"
        }
    }
}
```

The URL in the `url` key should point to the API url of your InfluxDB instance.

## Usage

### Read options

When reading data, most of the InfluxQL SELECT syntax is expressible through Juttle filter expressions.

Alternatively, raw queries are also available as a fallback.

Name | Type | Required | Description
-----|------|----------|-------------
`db`   | string | yes | database to use
`raw`  | string | no  | send a raw InfluxQL query to InfluxDB
`measurements` | array | no | measurements to query, defaults to all measurements
`offset` | integer| no | record offset
`limit`  | integer | no | query limit, defaults to 1000 records
`measurementField` | string | no | if specified, measurement name will be saved in a point field with this name
`from` | moment | no | select points after this time (inclusive)
`to`   | moment | no | select points before this time (exclusive)

### Write options

Name | Type | Required | Description
-----|------|----------|-------------
`db`   | string | yes | database to use
`intFields` | array | no | lists fields to be stored as integers instead of floats (default: none)
`valFields` | array | no | lists fields to be stored as values instead of tags (default: all non-string fields)
`measurement` | string | yes | measurement to use (required; optional if points contain measurement field as specified by the `measurementField` option)
`measurementField` | string | no | points will be checked for this field and its value will be used as the measurement name

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

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
