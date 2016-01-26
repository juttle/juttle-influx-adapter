'use strict';

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

    _fixture: function() {
        var payload = "";

        for (var i = 0; i < this._points; i++) {
            var t_i = this._t0 + i * this._dt;
            payload += 'cpu,host=host' + i + ' value=' + i + ' ' + t_i + '\n';
        }

        for (var j = 0; j < this._points; j++) {
            var t_j = this._t0 + j * this._dt;
            payload += 'mem,host=host' + j + ' value=' + j + ' ' + t_j + '\n';
        }

        return payload;
    },

    query: function(q) {
        var requestUrl = _.extend(influx_api_url, { pathname: '/query', query: { 'q': q, 'db': 'test' } });
        return request.async({ url: url.format(requestUrl), method: 'GET' }).then(this._handle_response).catch(function(e) {
            throw e;
        });
    },

    create: function() {
        return this.query('CREATE DATABASE test');
    },

    drop: function() {
        return this.query('DROP DATABASE test');
    },

    insert: function(data) {
        var payload = data || this._fixture();
        var requestUrl = _.extend(influx_api_url, { pathname: '/write', query: { 'db': 'test', 'precision': 'ms' } });

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
            DB.drop().then(function() { return DB.create(); }).then(function() { return DB.insert(); }).finally(done);
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
                expect(res.sinks.logger.length).to.equal(10);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('basic select', function() {
            return check_juttle({
                program: 'read influx -db "test" name = "cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(10);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('select across names', function() {
            return check_juttle({
                program: 'read influx -db "test" -nameField "name" name =~ /^(cpu|mem)$/ | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(20);
                expect(res.sinks.logger[0].time).to.equal(res.sinks.logger[1].time);

                _.each(res.sinks.logger, function(pt, i) {
                    expect(pt.name === 'cpu' || pt.name === 'mem').to.equal(true);
                });
            });
        });

        it('limit', function() {
            return check_juttle({
                program: 'read influx -db "test" -limit 5 name = "cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it('fields', function() {
            return check_juttle({
                program: 'read influx -db "test" -limit 1 -fields "value" name = "cpu" | view logger'
            }).then(function(res) {
                expect(_.keys(res.sinks.logger[0])).to.deep.equal(['time', 'value', 'name']);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('fields reports error if values not included', function() {
            return check_juttle({
                program: 'read influx -db "test" -limit 1 -fields "host" name = "cpu" | view logger'
            }).then(function(res) {
                expect(res.errors[0]).to.include('at least one field in select clause');
            });
        });

        it('from', function() {
            var from = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -from :' + from.toISOString() + ': name = "cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(8);
            });
        });

        it('to', function() {
            var to = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -to :' + to.toISOString() + ': name = "cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(2);
            });
        });

        it('from and to', function() {
            var from = new Date(DB._t0 + 2 * DB._dt);
            var to = new Date(DB._t0 + 5 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -from :' + from.toISOString() + ': -to :' + to.toISOString() + ': name = "cpu" | view logger'
            }).then(function(res) {
                expect(res.sinks.logger.length).to.equal(3);
            });
        });

        it('to before from throws', function() {
            var from = new Date(DB._t0 + 5 * DB._dt);
            var to = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: 'read influx -db "test" -from :' + from.toISOString() + ': -to :' + to.toISOString() + ': name = "cpu" | view logger'
            }).catch(function(err) {
                expect(err.message).to.include('From cannot be after to');
            });
        });

        it.skip('order by', function() {});

        describe('filters', function() {
            it('on tags', function() {
                return check_juttle({
                    program: 'read influx -db "test" host = "host1" name = "cpu" | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].host).to.equal('host1');
                });
            });

            it('on values', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" value = 5 | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('inequality on values', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" value > 8 | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(9);
                });
            });

            it('compound on tags', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" and (host = "host9" or host = "host1") | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].host).to.equal('host1');
                    expect(res.sinks.logger[1].host).to.equal('host9');
                });
            });

            it('compound on values', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" and (value = 1 or value = 5) | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            // Possibly bug in Influx, this actually returns all records
            it.skip('compound on tags and values', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" and (value = 1 or host = "host5") | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].host).to.equal('host5');
                });
            });

            it('not operator', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" and ( not ( value < 5 or value > 5 ) ) | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('in operator', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" and value in [1, 5] | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            it('not in', function() {
                return check_juttle({
                    program: 'read influx -db "test" name = "cpu" and (not ( value in [0, 1, 2, 3, 4] )) | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger.length).to.equal(5);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });
        });

        describe('nameField', function() {
            before(function(done) {
                var payload = 'namefield,host=hostX,name=conflict value=1 ' + DB._t0;
                DB.insert(payload).finally(done);
            });

            it('overwrites the name by default and triggers a warning', function() {
                return check_juttle({
                    program: 'read influx -db "test" -limit 1 name = "namefield" | view logger'
                }).then(function(res) {
                    expect(res.warnings[0]).to.include('Points contain name field');
                    expect(res.sinks.logger[0].name).to.equal('namefield');
                });
            });

            it('selects metric and stores its name based on nameField', function() {
                return check_juttle({
                    program: 'read influx -db "test" -nameField "metric" -limit 1 metric = "namefield" | view logger'
                }).then(function(res) {
                    expect(res.sinks.logger[0].name).to.equal('conflict');
                    expect(res.sinks.logger[0].metric).to.equal('namefield');
                });
            });
        });
    });

    describe('write', function() {
        beforeEach(function(done) {
            DB.drop().then(function() { return DB.create(); }).finally(done);
        });

        afterEach(function(done) {
            DB.drop().finally(done);
        });

        it('reports error on write to nonexistent db', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"name":"cpu"}] | write influx -db "doesnt_exist"'
            }).then(function(res) {
                expect(res.errors[0]).to.include('database not found');
            });
        });

        it('reports warning without name', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0}] | write influx -db "test"'
            }).then(function(res) {
                expect(res.warnings[0]).to.include('point is missing a name');
            });
        });

        it('point', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"name":"cpu"}] | write influx -db "test"'
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
                program: 'emit -points [{"time":"' + t.toISOString() + '","host":"host0","value":0,"name":"cpu"}] | write influx -db "test"'
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

        it('point with array triggers a warning', function() {
            return check_juttle({
                program: 'emit -limit 1 | put host = "host0", value = [1,2,3], name = "cpu" | write influx -db "test"'
            }).then(function(res) {
                expect(res.warnings.length).to.not.equal(0);
                expect(res.warnings[0]).to.include('not supported');
            });
        });

        it('point with object triggers a warning', function() {
            return check_juttle({
                program: 'emit -limit 1 | put host = "host0", value = {k:"v"}, name = "cpu" | write influx -db "test"'
            }).then(function(res) {
                expect(res.warnings.length).to.not.equal(0);
                expect(res.warnings[0]).to.include('not supported');
            });
        });

        it('valFields override', function() {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"str":"value","name":"cpu"}] | write influx -db "test" -valFields "str"'
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
                program: 'emit -points [{"host":"host0","value":0,"int_value":1,"name":"cpu"}] | write influx -db "test" -intFields "int_value"'
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

        it('can use name from the point', function() {
            return check_juttle({
                program: 'emit -points [{"m":"cpu","host":"host0","value":0}] | write influx -db "test" -nameField "m"'
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

        it('by default uses name field for name from the point', function() {
            return check_juttle({
                program: 'emit -points [{"name":"cpu","host":"host0","value":0}] | write influx -db "test"'
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
                program: 'emit -points [{"host":"host0","value":0,"time":"' + t1.toISOString() + '"},{"host":"host1","value":1,"time":" ' + t2.toISOString() + '"}] | put name = "cpu" | write influx -db "test"'
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
                program: 'emit -every :1ms: -points [{"host":"host0","value":0},{"n":"cpu","host":"host1","value":1}] | write influx -db "test" -nameField "n"'
            }).then(function(res) {
                expect(res.warnings.length).to.equal(1);
                expect(res.warnings[0]).to.include('point is missing a name');
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
