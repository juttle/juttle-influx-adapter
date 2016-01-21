'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var Juttle = require('juttle/lib/runtime').Juttle;

var Config = require('./config');
var Serializer = require('./serializer');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var Write = Juttle.proc.sink.extend({
    procName: 'write-influx',

    initialize: function(options, params) {
        this.name = 'write-influx';

        var allowed_options = ['raw', 'db', 'intFields', 'valFields', 'nameField', 'name'];
        var unknown = _.difference(_.keys(options), allowed_options);

        if (unknown.length > 0) {
            throw new Error('Unknown option ' + unknown[0]);
        }

        this.serializer = new Serializer(_.omit(options, 'raw', 'db'));

        this.db = options.db || 'test';
        this.url = Config.get().url;
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
                return null;
            }
        })).join("\n");

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
            } else {
                self.done();
            }
        }).catch(function(err) {
            self.trigger('error', err);
            self.done();
        });
    }
});

module.exports = Write;
