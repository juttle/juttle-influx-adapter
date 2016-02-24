'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');

/* global JuttleAdapterAPI */
var AdapterWrite = JuttleAdapterAPI.AdapterWrite;
var errors = JuttleAdapterAPI.errors;

var Config = require('./config');
var Serializer = require('./serializer');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

class WriteInflux extends AdapterWrite {
    constructor(options, params) {
        super(options, params);

        this.name = 'write-influx';

        this.serializer = new Serializer(_.pick(options, 'intFields', 'valFields', 'nameField'));

        this.nameField = options.nameField || 'name';
        this.db = options.db || 'test';
        this.url = Config.get().url;
        this.pendingWrites = 0;
    }

    // FIXME: Why doesn't write provide commonOptions, while read does?
    static allowedOptions() {
        return ['db', 'raw', 'nameField', 'intFields', 'valFields'];
    }

    static requiredOptions() {
        return ['db'];
    }

    write(points) {
        var parsedUrl = url.parse(this.url);
        var reqUrl;

        _.extend(parsedUrl, { pathname: '/write', query: { 'db': this.db, 'precision' : 'ms', } });

        reqUrl = url.format(parsedUrl);

        var body = _.compact(_.map(points, (p) => {
            try {
                return this.serializer.toInflux(p, this.nameField);
            } catch(err) {
                this.trigger('warning', errors.runtimeError('INTERNAL-ERROR', {
                    error: err.message
                }));
                return null;
            }
        })).join("\n");

        this.pendingWrites++;
        return request.async({
            url: reqUrl,
            method: 'POST',
            body
        }).then((response) => {
            // https://influxdb.com/docs/v0.9/guides/writing_data.html#writing-data-using-the-http-api
            // section http response summary
            if (response.statusCode === 200 || response.statusCode > 300) {
                throw errors.runtimeError('INTERNAL-ERROR', { error: response.body });
            }
        }).catch((err) => {
            this.trigger('error', err);
        }).then(() => {
            this.pendingWrites--;
            this.checkIfDone();
        });
    }

    eof() {
        if (this.pendingWrites === 0) {
            return Promise.resolve();
        } else {
            return new Promise((resolve, reject) => {
                this.onDone = resolve;
            });
        }
    }

    checkIfDone() {
        if (this.pendingWrites === 0 && this.onDone) {
            this.onDone();
        }
    }
}

module.exports = WriteInflux;
