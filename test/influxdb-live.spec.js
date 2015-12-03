var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var _ = require('underscore');

var expect = require('chai').expect;
var influxdb = require('../index.js');
var Juttle = require('juttle/lib/runtime').Juttle;
var url = require('url');

var Promise = require('bluebird');
global.Promise = Promise;

var fetch = require('isomorphic-fetch');

var influx_api_url = url.parse('http://localhost:8086/');

Juttle.backends.register('influxdb', influxdb({
    url: url.format(influx_api_url)
}, Juttle));

/* DB utils */
var DB = {
    _handle_response: function(response) {
        if (response.status !== 200 && response.status !== 204) {
            throw new Error(['error', response.status, response.statusText].join(' '));
        }
    },

    drop: function() {
        var requestUrl = _.extend(influx_api_url, { pathname: '/query', query: { 'q': 'DROP DATABASE test' } });
        return fetch(url.format(requestUrl)).then(this._handle_response).catch(function(e) {
           throw e;
        });
    },

    create: function() {
        var requestUrl = _.extend(influx_api_url, { pathname: '/query', query: { 'q': 'CREATE DATABASE test' } });
        return fetch(url.format(requestUrl)).then(this._handle_response).catch(function(e) {
            throw e;
        });
    },

    insert: function() {
        var payload = "";
        var requestUrl = _.extend(influx_api_url, { pathname: '/write', query: { 'db': 'test' } });

        for (var i = 0; i < 10; i++) {
            var t = Date.now() * 1000 + i * 100;
            payload += 'cpu,host=host' + i + ' value=' + i + ' ' + t + '\n';
        }

        return fetch(url.format(requestUrl), {
            method: 'post',
            body: payload
        }).then(this._handle_response).catch(function(e) {
            throw e;
        });
    },
};

describe('@live influxdb tests', function () {
    before(function(done) {
        DB.create().then(function() { return DB.insert(); }).finally(done);
    });

    after(function(done) {
        DB.drop().finally(done);
    });

    it('reports nonexistent database', function() {
        return check_juttle({
            program: 'read influxdb -db "doesnt_exist" -raw "SELECT * FROM /.*/"'
        }).then(function(res) {
            expect(res.errors[0]).to.include('database not found');
        });
    });

    describe('read', function() {
        it('-raw option', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -raw "SELECT * FROM cpu" | @logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(10)
                expect(res.sinks.logger[0].value).to.equal(0)
            });
        });

        it('basic select', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" | @logger'
            }).then(function(res) {
                expect(res.sinks.logger[0].value).to.equal(0)
            });
        });

        it('limit', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" -limit 5 | @logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it('fields', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" -limit 1 -fields "value" | @logger'
            }).then(function(res) {
                expect(_.keys(res.sinks.logger[0])).to.deep.equal(['time', 'value']);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('fields report if values not included', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" -limit 1 -fields "host" | @logger'
            }).then(function(res) {
                expect(res.errors[0]).to.include('at least one field in select clause');
            });
        });

        it.skip('from', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" | keep @logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it.skip('to', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" | keep @logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it.skip('from and to', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" | keep @logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it.skip('to before from', function() {
            return check_juttle({
                program: 'read influxdb -db "test" -measurements "cpu" | keep @logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it.skip('order by', function() {});

        describe('filters', function() {
            it('on tags', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" host = "host1" | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger[0].host).to.equal('host1');
                });
            });

            it('on values', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" value = 5 | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('inequality on values', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" value > 8 | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(9);
                });
            });

            it('compound on tags', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" host = "host9" or host = "host1" | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].host).to.equal('host1');
                    expect(res.sinks.logger[1].host).to.equal('host9');
                });
            });

            it('compound on values', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" value = 1 or value = 5 | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            // Possibly bug in Influx, this actually returns all records
            it.skip('compound on tags and values', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" value = 1 or host = "host5" | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].host).to.equal('host5');
                });
            });

            it('not operator', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" not ( value < 5 or value > 5 ) | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('in operator', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" value in [1, 5] | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            it('not in', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurements "cpu" not ( value in [0, 1, 2, 3, 4] ) | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(5);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });
        });

        describe('measurement', function() {
            it('stored in field specified by measurementField option', function() {
                return check_juttle({
                    program: 'read influxdb -db "test" -measurementField "measurement" -measurements "cpu" -limit 1 | @logger'
                }).then(function(res) {
                    expect(res.sinks.logger[0].measurement).to.equal('cpu');
                });
            });
        });
    });
});
