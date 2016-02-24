'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');

/* global JuttleAdapterAPI */
var JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
var AdapterRead = JuttleAdapterAPI.AdapterRead;

var Config = require('./config');
var Serializer = require('./serializer');
var QueryBuilder = require('./query');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

class ReadInflux extends AdapterRead {
    constructor(options, params) {
        super(options, params);

        if (options.raw) {
            if (params.filter_ast !== null) {
                throw this.compileError('INVALID-OPTION-COMBINATION', {
                    option: 'raw',
                    rule: 'empty filter'
                });
            }

            if (this.options.from || this.options.to) {
                throw this.compileError('OPTION-FROM-TO-ERROR', {
                    option: 'raw'
                });
            }

            this.from = undefined;
            this.to = undefined;
        } else {
            if (!this.options.from && !this.options.to) {
                throw this.compileError('MISSING-TIME-RANGE');
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
            _.pick(options, 'fields', 'raw'),
            this.queryOptimization.options || {},
            {
                nameField: this.nameField,
                limit: 1000,
            }
        );

        this.queryFilter  = params;
    }

    static allowedOptions() {
        return AdapterRead.commonOptions().concat(['db', 'optimize', 'raw', 'nameField', 'fields']);
    }

    static requiredOptions() {
        return ['db'];
    }

    setUrl(urlStr) {
        var urlObj = {};

        try {
            urlObj = url.parse(urlStr);
        } catch (e) {
            throw this.compileError('INVALID-URL-ERROR', { url: urlStr });
        }

        if (!urlObj.host) {
            throw this.compileError('INVALID-URL-ERROR', { url: urlStr });
        }

        return urlObj;
    }

    toNative(s) {
        var self = this;
        if (_.contains(s.columns, this.nameField)) {
            self.trigger('warning', this.runtimeError('INTERNAL-ERROR', {
                error: `Points contain ${this.nameField} field, use nameField option to make the field accessible.`
            }));
        }
        return _.map(s.values,
            (row) => this.serializer.toJuttle(s.name, s.columns, row, this.nameField)
        );
    }

    _sort(t1, t2) {
        return JuttleMoment.compare(t1.time, t2.time);
    }

    parse(data) {
        var e  = _.find(data.results, 'error');

        if (e && e.error) {
            throw this.runtimeError('INTERNAL-ERROR', { error: e.error });
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

    defaultTimeOptions() {
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

        this.logger.debug(`Running query: ${query}`);

        _.extend(parsedUrl, { pathname: '/query', query: { 'q': query, 'db': this.db, 'epoch' : 'ms' } });

        return request.async({
            url: url.format(parsedUrl),
            method: 'GET'
        }).then((response) => {
            if (response.statusCode < 200 || response.statusCode >= 300) {
                throw this.runtimeError('INTERNAL-ERROR', {
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
