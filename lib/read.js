'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var Juttle = require('juttle/lib/runtime').Juttle;

var Config = require('./config');
var Serializer = require('./serializer');
var QueryBuilder = require('./query');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var Read = Juttle.proc.source.extend({
    procName: 'read-influx',

    initialize: function(options, params, location, program) {
        var allowed_options = ['raw', 'db', 'offset', 'limit', 'fields', 'nameField', 'from', 'to'];
        var unknown = _.difference(_.keys(options), allowed_options);

        if (unknown.length > 0) {
            throw new Error('Unknown option ' + unknown[0]);
        }

        if (options.raw) {
            if (options.from || options.to) {
                throw new Error('-raw option should not be combined with -from, -to, or -last');
            }
        } else {
            if (!options.from && !options.to) {
                throw new Error('One of -from, -to, or -last must be specified to define a query time range');
            }

            options.from = options.from || program.now;
            options.to = options.to || program.now;

            if (JuttleMoment.gt(options.from, options.to)) {
                throw new Error('From cannot be after to');
            }
        }

        this.nameField = options.nameField || 'name';
        this.serializer = new Serializer();

        this.url = this.setUrl(Config.get().url);
        this.db = options.db;

        this.queryBuilder = new QueryBuilder();
        this.queryOptions = _.defaults(
            _.pick(options, 'offset', 'limit', 'fields', 'from', 'to'),
            {
                nameField: this.nameField,
                limit: 1000,
            }
        );
        this.queryFilter  = params;

        this.raw = options.raw;
    },

    setUrl: function(urlStr) {
        var urlObj = {};

        try {
            urlObj = url.parse(urlStr);
        } catch (e) {
            throw new this.runtime_error('RT-INVALID-URL-ERROR', { url: urlStr });
        }

        if (!urlObj.host) {
            throw new this.runtime_error('RT-INVALID-URL-ERROR', { url: urlStr });
        }

        return urlObj;
    },

    start: function() {
        var self = this;

        return this.fetch()
        .then(function(data) {
            self.emit(self.parse(data));
            self.emit_eof();
        }).catch(function(err) {
            self.trigger('error', err);
            self.emit_eof();
        });
    },

    toNative: function(s) {
        var self = this;
        if (_.contains(s.columns, this.nameField)) {
            self.trigger('warning',
                    new Error(
                        'Points contain '
                        + this.nameField
                        + ' field, use nameField option to make the field accessible.'
                        ));
        }
        return _.map(s.values, function(row) {
            return self.serializer.toJuttle(s.name, s.columns, row, self.nameField);
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
        return JuttleMoment.compare(t1.time, t2.time);
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
        var parsedUrl = _.clone(this.url);
        var reqUrl = null;

        var query = this.raw ? this.raw : this.queryBuilder.build(this.queryOptions, this.queryFilter);

        _.extend(parsedUrl, { pathname: '/query', query: { 'q': query, 'db': this.db, 'epoch' : 'ms' } });

        return request.async({
            url: url.format(parsedUrl),
            method: 'GET'
        }).then(function(response) {
            if (response.statusCode < 200 || response.statusCode >= 300) {
                throw new Error(response.statusCode + ': ' + response.body + ' for ' + reqUrl);
            }
            return JSON.parse(response.body);
        }).catch(function(e) {
            throw e;
        });
    },
});

module.exports = Read;
