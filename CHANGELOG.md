# Change Log

This file documents all notable changes to Juttle Influx Adapter. The release
numbering uses [semantic versioning](http://semver.org).

## 0.5.0

Released 2016-02-24

- Requires Juttle 0.5.0.

- Juttle is no longer included in `peerDependencies`, so manual installation is required.

- InfluxDB 0.10 support.

- `limit` and `offset` options were removed and `head` and `tail` procesors should
  be used to paginate the stream. Also, `head` and `tail` are now optimized by `read`.

- Writing `null` values now results in an error.

## 0.4.0

Released 2016-02-03

- Requires Juttle 0.4.0.

- Naming change - concept of a 'measurement' is consolidated under 'name'.

    - In read, `measurements` option was removed in favor of filtering using `name`.

    - In both read and write `measurementField` option was renamed to `nameField` and its default set to 'name'.

    - In write, `measurement` option was no longer needed and removed.

    - See [README.md](https://github.com/juttle/juttle-influx-adapter/blob/c177bd3f2aa15f6097fb97c858d6cfa7b2a80ba6/README.md) for examples and updated list of options.

## 0.3.0

Released 2016-01-20

- Requires Juttle 0.3.0.

- During write, points with object/array field values are dropped and emit a warning [[#31](https://github.com/juttle/juttle-influx-adapter/issues/31)].

## 0.2.0

Released 2016-01-07

- Requires Juttle 0.2.0.

- Document HTTP authentication support [[#22](https://github.com/juttle/juttle-influx-adapter/issues/22)].
