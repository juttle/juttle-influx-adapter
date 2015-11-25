var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;

global.Promise = Promise;
var fetch = require('isomorphic-fetch');

function InfluxBackend(config, Juttle) {
    var Write = Juttle.proc.sink.extend({
        procName: 'writex-influxdb',

        initialize: function(options, params) {
            this.name = 'writex-influxdb';

            var allowed_options = ['raw', 'db'];
            var unknown = _.difference(_.keys(options), allowed_options);

            if (unknown.length > 0) {
                throw this.compile_error('RT-UNKNOWN-OPTION-ERROR', {
                    proc: this.name,
                    option: unknown[0]
                });
            }

            this.db = options.db || 'test';
            this.url = config.url;
        },

        process: function(points) {
            var self = this;
            console.log(JSON.stringify(points));
            this.done();
        },

        toInflux: function(points) {
            return _.map(points, function(p) {
                [
                    // [key] + tags, comma separated, sorted by key, escape spaces and commas inside of all
                    // [fields]
                    // [timestamp]
                ].join(",")
            });
        }
    });

    var Read = Juttle.proc.base.extend({
        sourceType: 'batch',
        procName: 'readx-influxdb',

        initialize: function(options, params, pname, location, program, juttle) {
            var allowed_options = ['raw', 'db'];
            var unknown = _.difference(_.keys(options), allowed_options);

            if (unknown.length > 0) {
                throw this.compile_error('RT-UNKNOWN-OPTION-ERROR', {
                    proc: 'readx-influxdb',
                    option: unknown[0]
                });
            }

            this.db = options.db || 'test';
            this.table = options.table || '/.*/';
            this.url = config.url;
            this.raw = options.raw;
        },

        start: function() {
            var self = this;

            return this.fetch()
            .then(function(data) {
                self.emit(self.parse(data));
                self.emit_eof();
            })
            .catch(function(err) {
                self.trigger('error', err.message);
                self.logOnce('error', err.message);
                self.emit_eof();
            });
        },

        toNative: function(s) {
            return _.map(s.values, function(row) {
                // FIXME: Our sinks can't handle null/undefined?
                var obj  = _.object(s.columns, _.map(row, function(v) { return (v === null ? '' : v); }));
                var meta = { _name: s.name };

                obj.time = JuttleMoment.parse(obj.time);

                return _.extend(obj, meta);
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
            return JuttleMoment.compare('>', t1.time, t2.time) ? 1 : -1;
        },

        parse: function(data) {
            var e  = _.find(data.results, 'error');

            if (e && e.error) {
                throw this.runtime_error('RT-SOURCE-BACKEND-ERROR', { message: e.error });
            }

            var results = _.find(data.results, 'series') || {};

            if (!results.series) {
                return [];
            } else {
                return _.chain(results.series)
                    .map(this.toNative)
                    .flatten()
                    .sort(this._sort)
                    .value();
            }
        },

        fetch: function() {
            var self = this;
            var parsedUrl = url.parse(this.url);
            var reqUrl;

            _.extend(parsedUrl, { pathname: '/query', query: { 'q': this.raw, 'db': this.db, 'epoch' : 'ms' } });

            reqUrl = url.format(parsedUrl);

            if (!parsedUrl.host) {
                return Promise.reject(new this.runtime_error('RT-INVALID-URL-ERROR',
                    { url: reqUrl }
                ));
            } else {
                return fetch(reqUrl)
                    .then(function(response) {
                        if (response.status < 200 || response.status >= 300) {
                            throw new self.runtime_error('RT-INTERNAL-ERROR',
                                { error: response.status + ': ' + response.statusText + ' for ' + reqUrl }
                            );
                        }
                        return response.json();
                    });
            }
        },
    });

    return {
        name: 'influxdb',
        read: Read,
        write: Write
    };
}

module.exports = InfluxBackend;
