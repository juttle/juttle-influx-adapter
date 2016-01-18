var _ = require('underscore');
var expect = require('chai').expect;
var url = require('url');

var Promise = require('bluebird');
var retry = require('bluebird-retry');

var request = Promise.promisifyAll(require('request'));
request.async = Promise.promisify(request);

var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;

var influx = require('../index.js');

var retry_options = {
    interval: 50,
    timeout: 2000
};

var influx_api_url = {
    protocol: 'http',
    hostname: process.env.INFLUX_HOST || 'localhost',
    port: process.env.INFLUX_PORT || 8086,
    pathname: '/'
};

var Juttle = require('juttle/lib/runtime').Juttle;
Juttle.adapters.register('influx', influx({
    url: url.format(influx_api_url)
}, Juttle));

/* DB utils */
var DB = {
    // Influx doesnt seem to like writing future points?
    _t0: Date.now() - 3600 * 1000,
    _points: 10,
    _dt: 1000,

    _handle_response: function(response) {
        if (response.statusCode !== 200 && response.statusCode !== 204) {
            throw new Error(['error', response.statusCode, response.body].join(' '));
        }

        return response.body === "" ? null : JSON.parse(response.body);
    },

    query: function(q) {
        var requestUrl = _.extend(influx_api_url, { pathname: '/query', query: { 'q': q, 'db': 'test' } });
        return request.async({url: url.format(requestUrl), method: 'GET' }).then(this._handle_response).catch(function(e) {
            throw e;
        });
    },

    create: function() {
        return this.query('CREATE DATABASE test');
    },

    drop: function() {
        return this.query('DROP DATABASE test');
    },

    insert: function() {
        var payload = "";
        var requestUrl = _.extend(influx_api_url, { pathname: '/write', query: { 'db': 'test', 'precision': 'ms' } });

        for (var i = 0; i < this._points; i++) {
            var t = this._t0 + i * this._dt;
            payload += 'cpu,host=host' + i + ' value=' + i + ' ' + t + '\n';
        }

        return request.async({
            url: url.format(requestUrl),
            method: 'POST',
            body: payload
        }).then(this._handle_response).catch(function(e) {
            throw e;
        });
    },
};

describe('@live influxdb tests', function () {
    describe('read', function() {
        before(function(done) {
            DB.drop().then(function() { return DB.create() }).then(function() { return DB.insert(); }).finally(done);
        });

        after(function(done) {
            DB.drop().finally(done);
        });

        it('reports error on nonexistent database', function() {
            return check_juttle({
                program: 'read influx -db "doesnt_exist" -raw "SELECT * FROM /.*/"'
            }).then(function(res) {
                expect(res.errors[0]).to.include('database not found');
            });
        });

        it('-raw option', function() {
            return check_juttle({
                program: 'read influx -db "test" -raw "SELECT * FROM cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(10)
                expect(res.sinks.logger[0].value).to.equal(0)
            });
        });

        it('basic select', function() {
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(10)
                expect(res.sinks.logger[0].value).to.equal(0)
            });
        });

        it('limit', function() {
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -limit 5 | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it('fields', function() {
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -limit 1 -fields "value" | view logger'
            }).then(function(res) {
                expect(_.keys(res.sinks.logger[0])).to.deep.equal(['time', 'value']);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('fields reports error if values not included', function() {
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -limit 1 -fields "host" | view logger'
            }).then(function(res) {
                expect(res.errors[0]).to.include('at least one field in select clause');
            });
        });

        it('from', function() {
            var from = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -from :' + from.toISOString() + ': | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(8);
            });
        });

        it('to', function() {
            var to = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -to :' + to.toISOString() + ': | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(2);
            });
        });

        it('from and to', function() {
            var from = new Date(DB._t0 + 2 * DB._dt);
            var to = new Date(DB._t0 + 5 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -from :' + from.toISOString() + ': -to :' + to.toISOString() + ': | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(3);
            });
        });

        it('to before from throws', function() {
            var from = new Date(DB._t0 + 5 * DB._dt);
            var to = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -measurements "cpu" -from :' + from.toISOString() + ': -to :' + to.toISOString() + ': | view logger'
            }).catch(function(err) {
                expect(err.message).to.include('From cannot be after to');
            });
        });

        it.skip('order by', function() {});

        describe('filters', function() {
            it('on tags', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" host = "host1" | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1)
                    expect(res.sinks.logger[0].host).to.equal('host1');
                });
            });

            it('on values', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" value = 5 | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1)
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('inequality on values', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" value > 8 | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(9);
                });
            });

            it('compound on tags', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" host = "host9" or host = "host1" | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].host).to.equal('host1');
                    expect(res.sinks.logger[1].host).to.equal('host9');
                });
            });

            it('compound on values', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" value = 1 or value = 5 | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            // Possibly bug in Influx, this actually returns all records
            it.skip('compound on tags and values', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" value = 1 or host = "host5" | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].host).to.equal('host5');
                });
            });

            it('not operator', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" not ( value < 5 or value > 5 ) | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('in operator', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" value in [1, 5] | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            it('not in', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurements "cpu" not ( value in [0, 1, 2, 3, 4] ) | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(5);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });
        });

        describe('measurement', function() {
            it('stored in field specified by measurementField option', function() {
                return check_juttle({
                    program: 'read influx -db "test" -measurementField "measurement" -measurements "cpu" -limit 1 | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger[0].measurement).to.equal('cpu');
                });
            });
        });
    });

    describe('write', function() {
        beforeEach(function(done) {
            DB.drop().then(function() { return DB.create() }).finally(done);
        });

        afterEach(function(done) {
            DB.drop().finally(done);
        });

        it('reports error on write to nonexistent db', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0}] | write influx -db "doesnt_exist" -measurement "cpu"'
            }).then(function(res) {
                expect(res.errors[0]).to.include('database not found');
            });
        });

        it('reports warning without measurement', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0}] | write influx -db "test"'
            }).then(function(res) {
                expect(res.warnings[0]).to.include('point is missing a measurement');
            });
        });

        it('point', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0}] | write influx -db "test" -measurement "cpu"'
            }).then(function(res) {
                return retry(function() {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then(function(json) {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('point with time', function() {
            var t = new Date(Date.now());
            return check_juttle({
                program: 'emit -points [{"time":"' + t.toISOString() + '","host":"host0","value":0}] | write influx -db "test" -measurement "cpu"'
            }).then(function(res) {
                return retry(function() {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then(function(json) {
                        var data = json.results[0].series[0];
                        expect(new Date(data.values[0][0]).toISOString()).to.equal(t.toISOString());
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('valFields override', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"str":"value"}] | write influx -db "test" -measurement "cpu" -valFields "str"'
            }).then(function(res) {
                return retry(function() {
                    return DB.query('SHOW FIELD KEYS').then(function(json) {
                        var fields = _.flatten(json.results[0].series[0].values);
                        expect(fields).to.include('str');
                    });
                }, retry_options);
            });
        });

        it('intFields override', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"int_value":1}] | write influx -db "test" -measurement "cpu" -intFields "int_value"'
            }).then(function(res) {
                return retry(function() {
                    return DB.query('SELECT * FROM cpu WHERE int_value = 1').then(function(json) {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(1);
                        expect(data.values[0][3]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('can use measurement from the point', function() {
            return check_juttle({
                program: 'emit -points [{"m":"cpu","host":"host0","value":0}] | write influx -db "test" -measurementField "m"'
            }).then(function(res) {
                return retry(function() {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then(function(json) {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('two points', function() {
            // This is a workaround for emit adding same time in ms to both points
            // and influx treating time as primary index, overwriting the points
            var t1 = new Date(Date.now());
            var t2 = new Date(Date.now() - 1000);

            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"time":"' + t1.toISOString() + '"},{"host":"host1","value":1,"time":" ' + t2.toISOString() + '"}] | write influx -db "test" -measurement "cpu"'
            }).then(function(res) {
                return retry(function() {
                    return DB.query('SELECT * FROM cpu').then(function(json) {
                        var data = json.results[0].series[0];
                        expect(data.values.length).to.equal(2);
                    });
                }, retry_options);
            });
        });

        it('emits a warning on serialization but continues', function() {
            return check_juttle({
                program: 'emit -every :1ms: -points [{"host":"host0","value":0},{"m":"cpu","host":"host1","value":1}] | write influx -db "test" -measurementField "m"'
            }).then(function(res) {
                expect(res.warnings.length).to.equal(1);
                expect(res.warnings[0]).to.include('point is missing a measurement');
                return retry(function() {
                    return DB.query('SELECT * FROM cpu WHERE value = 1').then(function(json) {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host1");
                        expect(data.values[0][2]).to.equal(1);
                    });
                }, retry_options);
            });
        });
    });
});
