'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var AdapterRead = require('juttle/lib/runtime/adapter-read');
var errors = require('juttle/lib/errors');

var Config = require('./config');
var Serializer = require('./serializer');
var QueryBuilder = require('./query');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

class ReadInflux extends AdapterRead {
    constructor(options, params) {
        super(options, params);

        var allowed_options = AdapterRead.commonOptions.concat([
            'raw', 'db', 'offset', 'limit', 'fields', 'nameField', 'optimize'
        ]);
        var unknown = _.difference(_.keys(options), allowed_options);

        if (unknown.length > 0) {
            throw errors.compileError('RT-UNKNOWN-OPTION-ERROR', {
                proc: 'read influx',
                option: unknown[0]
            });
        }

        if (options.raw) {
            if (this.options.from || this.options.to) {
                throw errors.compileError('RT-OPTION-FROM-TO-ERROR', {
                    option: 'raw'
                });
            }

            this.from = undefined;
            this.to = undefined;
        } else {
            if (!this.options.from && !this.options.to) {
                if (!this.options.from && !this.options.to) {
                    throw errors.compileError('RT-MISSING-TIME-RANGE-ERROR');
                }
            }

            this.from = this.options.from || params.now;
            this.to = this.options.to || params.now;
        }

        this.nameField = options.nameField || 'name';
        this.serializer = new Serializer();

        this.url = this.setUrl(Config.get().url);
        this.db = options.db;

        this.queryBuilder = new QueryBuilder();
        this.queryOptimization = params.optimization_info;

        this.queryOptions = _.defaults(
            _.pick(options, 'offset', 'limit', 'fields', 'raw'),
            this.queryOptimization.options || {},
            {
                nameField: this.nameField,
                limit: 1000,
            }
        );

        this.queryFilter  = params;
    }

    setUrl(urlStr) {
        var urlObj = {};

        try {
            urlObj = url.parse(urlStr);
        } catch (e) {
            throw errors.compileError('RT-INVALID-URL-ERROR', { url: urlStr });
        }

        if (!urlObj.host) {
            throw errors.compileError('RT-INVALID-URL-ERROR', { url: urlStr });
        }

        return urlObj;
    }

    toNative(s) {
        var self = this;
        if (_.contains(s.columns, this.nameField)) {
            self.trigger('warning', errors.runtimeError('RT-INTERNAL-ERROR', {
                error: `Points contain ${this.nameField} field, use nameField option to make the field accessible.`
            }));
        }
        return _.map(s.values,
            (row) => this.serializer.toJuttle(s.name, s.columns, row, this.nameField)
        );
    }

    _sort(t1, t2) {
        if (!_.has(t1, 'time') && !_.has(t2, 'time')) {
            return 0;
        }
        if (!_.has(t1, 'time')) {
            return -1;
        }
        if (!_.has(t2, 'time')) {
            return 1;
        }
        return JuttleMoment.compare(t1.time, t2.time);
    }

    parse(data) {
        var e  = _.find(data.results, 'error');

        if (e && e.error) {
            throw errors.runtimeError('RT-INTERNAL-ERROR', { error: e.error });
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
    }

    defaultTimeRange() {
        return {
            from: this.from,
            to: this.to
        };
    }

    periodicLiveRead() {
        return this.queryOptions.raw === undefined;
    }

    read(from, to, limit, state) {
        var self = this;

        var parsedUrl = _.clone(this.url);
        var reqUrl = null;

        var queryOptions = _.extend({ to, from }, this.queryOptions);
        var query = this.queryBuilder.build(queryOptions, this.queryFilter);

        _.extend(parsedUrl, { pathname: '/query', query: { 'q': query, 'db': this.db, 'epoch' : 'ms' } });

        return request.async({
            url: url.format(parsedUrl),
            method: 'GET'
        }).then((response) => {
            if (response.statusCode < 200 || response.statusCode >= 300) {
                throw errors.runtimeError('RT-INTERNAL-ERROR', {
                    error: `${response.statusCode}: ${response.body} for ${reqUrl}`
                });
            }

            return {
                points: self.parse(JSON.parse(response.body)),
                readEnd: to || new JuttleMoment(Infinity)
            };
        }).catch((e) => {
            throw e;
        });
    }
}

module.exports = ReadInflux;
