'use strict';

var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');

/* global JuttleAdapterAPI */
var AdapterWrite = JuttleAdapterAPI.AdapterWrite;

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

        // Writes need to be chunked, ~200k point bulk writes result in influx
        // timing out. Let's be conservative.
        this.writeChunkSize = 10000;
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

        var rows = this.serialize(points);

        var chunks = this.chunkRows(rows, this.writeChunkSize);

        return this.sendWrites(reqUrl, chunks);
    }

    serialize(points) {
        return _.compact(_.map(points, (p) => {
            try {
                return this.serializer.toInflux(p, this.nameField);
            } catch(err) {
                this.trigger('warning', this.runtimeError('INTERNAL-ERROR', {
                    error: err.message
                }));
                return null;
            }
        }));
    }

    sendWrites(url, chunks) {
        this.pendingWrites++;

        return Promise.each(chunks, (chunk) => {
            return this.request(url, chunk);
        }).catch((err) => {
            this.trigger('error', err);
        }).then(() => {
            this.pendingWrites--;
            this.checkIfDone();
        });
    }

    chunkRows(body, writeChunkSize) {
        var writeChunks = [];
        while (body.length > 0) {
            writeChunks.push(body.splice(0, writeChunkSize));
        }
        return writeChunks;
    }

    request(url, chunk) {
        return request.async({
            url: url,
            method: 'POST',
            body: chunk.join('\n')
        }).then((response) => {
            // https://influxdb.com/docs/v0.9/guides/writing_data.html#writing-data-using-the-http-api
            // section http response summary
            if (response.statusCode === 200 || response.statusCode > 300) {
                throw this.runtimeError('INTERNAL-ERROR', { error: response.body });
            }
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
