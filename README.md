# InfluxDB Backend for Juttle

FIXME: build status

## About

This package provides a pluggable InfluxDB backend for Juttle data flow
language. It supports querying data stored in Influx (large part of InfluxQL
SELECT syntax is accessible through options and raw mode is also available as a
fallback). As for writing, it can store arbitrary non-nested data (shallow
objects).

Currently, only InfluxDB 0.9.0 and newer, JSON (reading) and line protocol
over HTTP (writing) is supported.

## Installation

This assumes you have Juttle already installed. You can test that by running:

    $ npm ls -g | grep juttle

if the output contains `juttle`, you're good to go.  Alternatively, go to
[Juttle repository]() and follow the installation instructions.

Then install the backend package:

    npm install -g juttle-influxdb-backend

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

FIXME: authentication support.

## Usage

FIXME: backends should support listing options

    $ juttle -e 'readx influxdb -db "test"'

Reads all points from the `test` database.

    $ juttle -e 'emit -points [{"value": 0.01}] | writex influxdb -db "test" -measurement "cpu"

Stores a single point in the `cpu` measurement of `test` database.

When storing points, following conventions are used:

1. All keys, whose values are strings, are treated as [tags](). You can always
   override this behavior using `valueFields` option:
   `...|writex influxdb -valueFields "foo","bar"` will treat `foo` and `bar`
   as fields, not tags.

2. All numeric fields are stored as floats. This can be changed by enumerating
   the integer fields via `intFields` option.

For more complex examples, please take a look at Juttle scripts in the [examples]().

## Contributing

Want to contribute? Awesome! We encourage everyone to contribute to Juttle and
its backends.

We put up a guide over here FIXME.

To contribute to InfluxDB backend directly, please see this guide on how to get
started.

## License

All files in this repository are licensed under Apache 2.0 License.
