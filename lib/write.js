'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var AdapterWrite = require('juttle/lib/runtime/adapter-write');

var Config = require('./config');
var Serializer = require('./serializer');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

class Write extends AdapterWrite {
    constructor(options, params) {
        super(options, params);

        this.name = 'write-influx';

        var allowed_options = ['raw', 'db', 'intFields', 'valFields', 'nameField'];
        var unknown = _.difference(_.keys(options), allowed_options);

        if (unknown.length > 0) {
            throw new Error('Unknown option ' + unknown[0]);
        }

        this.serializer = new Serializer(_.pick(options, 'intFields', 'valFields', 'nameField'));

        this.nameField = options.nameField || 'name';
        this.db = options.db || 'test';
        this.url = Config.get().url;
        this.pendingWrites = 0;
    }

    write(points) {
        var self = this;

        var parsedUrl = url.parse(this.url);
        var reqUrl;

        _.extend(parsedUrl, { pathname: '/write', query: { 'db': this.db, 'precision' : 'ms', } });

        reqUrl = url.format(parsedUrl);

        var body = _.compact(_.map(points, function(p) {
            try {
                return self.serializer.toInflux(p, self.nameField);
            } catch(err) {
                self.trigger('warning', err);
                return null;
            }
        })).join("\n");

        this.pendingWrites++;
        return request.async({
            url: reqUrl,
            method: 'POST',
            body: body
        }).then(function(response) {
            // https://influxdb.com/docs/v0.9/guides/writing_data.html#writing-data-using-the-http-api
            // section http response summary
            if (response.statusCode === 200) {
                throw new Error(response.body);
            } else if (response.statusCode > 300) {
                throw new Error(response.body);
            }
        }).catch(function(err) {
            self.trigger('error', err);
        }).then(function() {
            self.pendingWrites--;
            self.checkIfDone();
        });
    }

    eof() {
        var self = this;
        if (this.pendingWrites === 0) {
            return Promise.resolve();
        } else {
            return new Promise(function(resolve, reject) {
                self.onDone = resolve;
            });
        }
    }

    checkIfDone() {
        if (this.pendingWrites === 0 && this.onDone) {
            this.onDone();
        }
    }
}

module.exports = Write;
