var _ = require('underscore');
var url = require('url');
var Promise = require('bluebird');
var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var DB = {
    // Influx doesnt seem to like writing future points?
    _t0: Date.now() - 3600 * 1000,
    _points: 10,
    _dt: 1000,

    init(urlString) {
        this.url = url.parse(urlString);
        return this;
    },

    _handle_response(response) {
        if (response.statusCode !== 200 && response.statusCode !== 204) {
            throw new Error(['error', response.statusCode, response.body].join(' '));
        }

        return response.body === "" ? null : JSON.parse(response.body);
    },

    _fixture() {
        var payload = "";

        for (var i = 0; i < this._points; i++) {
            var t_i = this._t0 + i * this._dt;
            payload += `cpu,host=host${i} value=${i} ${t_i}\n`;
        }

        for (var j = 0; j < this._points; j++) {
            var t_j = this._t0 + j * this._dt;
            payload += `mem,host=host${j} value=${j} ${t_j}\n`;
        }

        return payload;
    },

    query(q) {
        var requestUrl = _.extend(this.url, { pathname: '/query', query: { q, 'db': 'test' } });
        return request.async({
            url: url.format(requestUrl),
            method: 'GET'
        }).then(this._handle_response).catch((e) => {
            throw e;
        });
    },

    create() {
        return this.query('CREATE DATABASE test');
    },

    drop() {
        return this.query('DROP DATABASE test');
    },

    insert(data) {
        var payload = data || this._fixture();
        var requestUrl = _.extend(this.url, { pathname: '/write', query: { 'db': 'test', 'precision': 'ms' } });

        return request.async({
            url: url.format(requestUrl),
            method: 'POST',
            body: payload
        }).then(this._handle_response).catch((e) => {
            throw e;
        });
    },
};

module.exports = DB;
