var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var Juttle = require('juttle/lib/runtime').Juttle;

global.Promise = Promise;
var fetch = require('isomorphic-fetch');

var Serializer = require('./lib/serializer');
var QueryBuilder = require('./lib/query');

var config;

var Write = Juttle.proc.sink.extend({
    procName: 'write-influxdb',

    initialize: function(options, params) {
        this.name = 'write-influxdb';

        var allowed_options = ['raw', 'db', 'intFields', 'valFields', 'measurementField', 'measurement'];
        var unknown = _.difference(_.keys(options), allowed_options);

        if (unknown.length > 0) {
            throw new Error('Unknown option ' + unknown[0]);
        }

        this.serializer = new Serializer(_.omit(options, 'raw', 'db'));

        this.db = options.db || 'test';
        this.url = config.url;
    },

    process: function(points) {
        var self = this;

        var parsedUrl = url.parse(this.url);
        var reqUrl;

        _.extend(parsedUrl, { pathname: '/write', query: { 'db': this.db, 'precision' : 'ms', } });

        reqUrl = url.format(parsedUrl);

        var body = _.compact(_.map(points, function(p) {
            try {
                return self.serializer.toInflux(p);
            } catch(err) {
                self.trigger('warning', err);
                self.logOnce('error', err.message);
                return null;
            }
        })).join("\n");

        return fetch(reqUrl, {
            method: 'post',
            body: body
        }).then(function(response) {
            // https://influxdb.com/docs/v0.9/guides/writing_data.html#writing-data-using-the-http-api
            // section http response summary
            if (response.status === 200) {
                throw new Error(response.text());
            } else if (response.status > 300) {
                throw new Error(response.text());
            } else {
                self.done();
            }
        }).catch(function(err) {
            self.trigger('error', err);
            self.logOnce('error', err.message);
            self.done();
        });
    }
});

var Read = Juttle.proc.base.extend({
    sourceType: 'batch',
    procName: 'read-influxdb',

    initialize: function(options, params, pname, location, program, juttle) {
        var allowed_options = ['raw', 'db', 'measurements', 'offset', 'limit', 'fields', 'measurementField'];
        var unknown = _.difference(_.keys(options), allowed_options);

        if (unknown.length > 0) {
            throw new Error('Unknown option ' + unknown[0]);
        }

        this.serializer = new Serializer(_.pick(options, 'measurementField'));

        this.url = config.url;
        this.db = options.db;

        this.queryBuilder = new QueryBuilder();
        this.queryOptions = _.defaults(
            _.pick(options, 'measurements', 'offset', 'limit', 'fields'),
            {
                limit: 1000,
            }
        );
        this.queryFilter  = params;

        this.raw = options.raw;
    },

    start: function() {
        var self = this;

        return this.fetch()
        .then(function(data) {
            self.emit(self.parse(data));
            self.emit_eof();
        }).catch(function(err) {
            self.trigger('error', err);
            self.logOnce('error', err.message);
            self.emit_eof();
        });
    },

    toNative: function(s) {
        var self = this;
        return _.map(s.values, function(row) {
            return self.serializer.toJuttle(s.name, s.columns, row);
        });
    },

    _sort: function(t1, t2) {
        if (!_.has(t1, 'time') && !_.has(t2, 'time')) {
            return 0;
        }
        if (!_.has(t1, 'time')) {
            return -1;
        }
        if (!_.has(t2, 'time')) {
            return 1;
        }
        // FIXME: doesn't handle equal moments
        return JuttleMoment.compare('>', t1.time, t2.time) ? 1 : -1;
    },

    parse: function(data) {
        var e  = _.find(data.results, 'error');

        if (e && e.error) {
            throw new Error(e.error);
        }

        var results = _.find(data.results, 'series') || {};

        if (!results.series) {
            return [];
        } else {
            return _.chain(results.series)
                .map(this.toNative.bind(this))
                .flatten()
                .sort(this._sort)
                .value();
        }
    },

    fetch: function() {
        var self = this;
        var parsedUrl = url.parse(this.url);
        var reqUrl;

        var query = this.raw ? this.raw : this.queryBuilder.build(this.queryOptions, this.queryFilter);

        _.extend(parsedUrl, { pathname: '/query', query: { 'q': query, 'db': this.db, 'epoch' : 'ms' } });

        reqUrl = url.format(parsedUrl);

        if (!parsedUrl.host) {
            return Promise.reject(new this.runtime_error('RT-INVALID-URL-ERROR',
                { url: reqUrl }
            ));
        } else {
            return fetch(reqUrl)
                .then(function(response) {
                    if (response.status < 200 || response.status >= 300) {
                        throw new Error(response.status + ': ' + response.statusText + ' for ' + reqUrl);
                    }
                    return response.json();
                }).catch(function(e) {
                    throw e;
                });
        }
    },
});

function InfluxBackend(cfg) {
    config = cfg;
    return {
        name: 'influxdb',
        read: Read,
        write: Write
    };
}

module.exports = InfluxBackend;
