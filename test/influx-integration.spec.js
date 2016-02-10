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
        var requestUrl = _.extend(influx_api_url, { pathname: '/query', query: { q, 'db': 'test' } });
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
        var requestUrl = _.extend(influx_api_url, { pathname: '/write', query: { 'db': 'test', 'precision': 'ms' } });

        return request.async({
            url: url.format(requestUrl),
            method: 'POST',
            body: payload
        }).then(this._handle_response).catch((e) => {
            throw e;
        });
    },
};

describe('@integration influxdb tests', () => {
    describe('read', () => {
        before((done) => {
            DB.drop().then(() => { return DB.create(); }).then(() => { return DB.insert(); }).finally(done);
        });

        after((done) => {
            DB.drop().finally(done);
        });

        it('reports error on nonexistent database', () => {
            return check_juttle({
                program: 'read influx -db "doesnt_exist" -raw "SELECT * FROM /.*/"'
            }).then((res) => {
                expect(res.errors[0]).to.include('database not found');
            });
        });

        it('-raw option', () => {
            return check_juttle({
                program: 'read influx -db "test" -raw "SELECT * FROM cpu" | view logger'
            }).then((res) => {
                expect(res.errors.length).to.equal(0);
                expect(res.sinks.logger.length).to.equal(10);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('reports error with -raw and -from', () => {
            return check_juttle({
                program: 'read influx -db "test" -from :0: -raw "SELECT * FROM cpu" | view logger'
            }).catch((err) => {
                expect(err.message).to.include('-raw option should not be combined with -from, -to, or -last');
            });
        });

        it('basic select', () => {
            return check_juttle({
                program: 'read influx -db "test" -from :0: name = "cpu" | view logger'
            }).then((res) => {
                expect(res.sinks.logger.length).to.equal(10);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('select across names', () => {
            return check_juttle({
                program: 'read influx -db "test" -from :0: -nameField "name" name =~ /^(cpu|mem)$/ | view logger'
            }).then((res) => {
                expect(res.sinks.logger.length).to.equal(20);
                expect(res.sinks.logger[0].time).to.equal(res.sinks.logger[1].time);

                _.each(res.sinks.logger, (pt, i) => {
                    expect(pt.name === 'cpu' || pt.name === 'mem').to.equal(true);
                });
            });
        });

        it('limit', () => {
            return check_juttle({
                program: 'read influx -db "test" -from :0: -limit 5 name = "cpu" | view logger'
            }).then((res) => {
                expect(res.sinks.logger.length).to.equal(5);
            });
        });

        it('fields', () => {
            return check_juttle({
                program: 'read influx -db "test" -from :0: -limit 1 -fields "value" name = "cpu" | view logger'
            }).then((res) => {
                expect(_.keys(res.sinks.logger[0])).to.deep.equal(['time', 'value', 'name']);
                expect(res.sinks.logger[0].value).to.equal(0);
            });
        });

        it('fields reports error if values not included', () => {
            return check_juttle({
                program: 'read influx -db "test" -from :0: -limit 1 -fields "host" name = "cpu" | view logger'
            }).then((res) => {
                expect(res.errors[0]).to.include('at least one field in select clause');
            });
        });

        it('from', () => {
            var from = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: `read influx -db "test" -from :${from.toISOString()}: name = "cpu" | view logger`
            }).then((res) => {
                expect(res.sinks.logger.length).to.equal(8);
            });
        });

        it('to', () => {
            var to = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: `read influx -db "test" -from :0: -to :${to.toISOString()}: name = "cpu" | view logger`
            }).then((res) => {
                expect(res.errors).deep.equal([]);
                expect(res.sinks.logger.length).to.equal(2);
            });
        });

        it('from and to', () => {
            var from = new Date(DB._t0 + 2 * DB._dt);
            var to = new Date(DB._t0 + 5 * DB._dt);
            return check_juttle({
                program: `read influx -db "test" -from :${from.toISOString()}: -to :${to.toISOString()}: name = "cpu" | view logger`
            }).then((res) => {
                expect(res.sinks.logger.length).to.equal(3);
            });
        });

        it('to before from throws', () => {
            var from = new Date(DB._t0 + 5 * DB._dt);
            var to = new Date(DB._t0 + 2 * DB._dt);
            return check_juttle({
                program: `read influx -db "test" -from :${from.toISOString()}: -to :${to.toISOString()}: name = "cpu" | view logger`
            }).catch((err) => {
                expect(err.message).to.include('-to must not be earlier than -from');
            });
        });

        describe('filters', () => {
            it('on tags', () => {
                return check_juttle({
                    program: 'read influx -db "test"  -from :0: host = "host1" name = "cpu" | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].host).to.equal('host1');
                });
            });

            it('on values', () => {
                return check_juttle({
                    program: 'read influx -db "test"  -from :0: name = "cpu" value = 5 | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('inequality on values', () => {
                return check_juttle({
                    program: 'read influx -db "test"  -from :0: name = "cpu" value > 8 | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(9);
                });
            });

            it('compound on tags', () => {
                return check_juttle({
                    program: 'read influx -db "test"  -from :0: name = "cpu" and (host = "host9" or host = "host1") | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].host).to.equal('host1');
                    expect(res.sinks.logger[1].host).to.equal('host9');
                });
            });

            it('compound on values', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: name = "cpu" and (value = 1 or value = 5) | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            // Possibly bug in Influx, this actually returns all records
            it.skip('compound on tags and values', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: name = "cpu" and (value = 1 or host = "host5") | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].host).to.equal('host5');
                });
            });

            it('not operator', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: name = "cpu" and ( not ( value < 5 or value > 5 ) ) | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(1);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });

            it('in operator', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: name = "cpu" and value in [1, 5] | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(2);
                    expect(res.sinks.logger[0].value).to.equal(1);
                    expect(res.sinks.logger[1].value).to.equal(5);
                });
            });

            it('not in', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: name = "cpu" and (not ( value in [0, 1, 2, 3, 4] )) | view logger'
                }).then((res) => {
                    expect(res.sinks.logger.length).to.equal(5);
                    expect(res.sinks.logger[0].value).to.equal(5);
                });
            });
        });

        describe('nameField', () => {
            before((done) => {
                var payload = `namefield,host=hostX,name=conflict value=1 ${DB._t0}`;
                DB.insert(payload).finally(done);
            });

            it('overwrites the name by default and triggers a warning', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: -limit 1 name = "namefield" | view logger'
                }).then((res) => {
                    expect(res.warnings).to.deep.equal(['internal error Points contain name field, use nameField option to make the field accessible.']);
                    expect(res.sinks.logger[0].name).to.equal('namefield');
                });
            });

            it('selects metric and stores its name based on nameField', () => {
                return check_juttle({
                    program: 'read influx -db "test" -from :0: -nameField "metric" -limit 1 metric = "namefield" | view logger'
                }).then((res) => {
                    expect(res.sinks.logger[0].name).to.equal('conflict');
                    expect(res.sinks.logger[0].metric).to.equal('namefield');
                });
            });
        });

        describe('optimizations', () => {
            describe('head', () => {
                it('head n', () => {
                    return check_juttle({
                        program: 'read influx -db "test" -from :0: name = "cpu" | head 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(3);
                    });
                });

                it('head n, unoptimized', () => {
                    return check_juttle({
                        program: 'read influx -optimize false -db "test" -from :0: name = "cpu" | head 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(3);
                    });
                });

                it('head n doesnt override limit', () => {
                    return check_juttle({
                        program: 'read influx -db "test" -limit 1 -from :0: name = "cpu" | head 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(1);
                    });
                });

                it('head n doesnt override limit, unoptimized', () => {
                    return check_juttle({
                        program: 'read influx -optimize false -db "test" -limit 1 -from :0: name = "cpu" | head 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(1);
                    });
                });
            });

            describe('tail', () => {
                it('tail n', () => {
                    return check_juttle({
                        program: 'read influx -db "test" -from :0: name = "cpu" | tail 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(3);
                        expect(res.sinks.logger[0].value).to.equal(7);
                        expect(res.sinks.logger[2].value).to.equal(9);
                    });
                });

                it('tail n, unoptimized', () => {
                    return check_juttle({
                        program: 'read influx -optimize false -db "test" -from :0: name = "cpu" | tail 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(3);
                        expect(res.sinks.logger[0].value).to.equal(7);
                        expect(res.sinks.logger[2].value).to.equal(9);
                    });
                });

                it('tail n doesnt override limit', () => {
                    return check_juttle({
                        program: 'read influx -db "test" -limit 1 -from :0: name = "cpu" | tail 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(1);
                    });
                });

                it('tail n doesnt override limit, unoptimized', () => {
                    return check_juttle({
                        program: 'read influx -optimize false -db "test" -limit 1 -from :0: name = "cpu" | tail 3 | view logger'
                    }).then((res) => {
                        expect(res.sinks.logger.length).to.equal(1);
                    });
                });
            });
        });

    });

    describe('write', () => {
        beforeEach((done) => {
            DB.drop().then(() => { return DB.create(); }).finally(done);
        });

        afterEach((done) => {
            DB.drop().finally(done);
        });

        it('reports error on write to nonexistent db', () => {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"name":"cpu"}] | write influx -db "doesnt_exist"'
            }).then((res) => {
                expect(res.errors[0]).to.include('database not found');
            });
        });

        it('reports warning without name', () => {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0}] | write influx -db "test"'
            }).then((res) => {
                expect(res.warnings[0]).to.include('point is missing a name');
            });
        });

        it('point', () => {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"name":"cpu"}] | write influx -db "test"'
            }).then((res) => {
                return retry(() => {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then((json) => {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('point with time', () => {
            var t = new Date(Date.now());
            return check_juttle({
                program: `emit -points [{"time":"${t.toISOString()}","host":"host0","value":0,"name":"cpu"}] | write influx -db "test"`
            }).then((res) => {
                return retry(() => {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then((json) => {
                        var data = json.results[0].series[0];
                        expect(new Date(data.values[0][0]).toISOString()).to.equal(t.toISOString());
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('point with array triggers a warning', () => {
            return check_juttle({
                program: 'emit -limit 1 | put host = "host0", value = [1,2,3], name = "cpu" | write influx -db "test"'
            }).then((res) => {
                expect(res.warnings.length).to.not.equal(0);
                expect(res.warnings[0]).to.include('not supported');
            });
        });

        it('point with object triggers a warning', () => {
            return check_juttle({
                program: 'emit -limit 1 | put host = "host0", value = {k:"v"}, name = "cpu" | write influx -db "test"'
            }).then((res) => {
                expect(res.warnings.length).to.not.equal(0);
                expect(res.warnings[0]).to.include('not supported');
            });
        });

        it('valFields override', () => {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"str":"value","name":"cpu"}] | write influx -db "test" -valFields "str"'
            }).then((res) => {
                return retry(() => {
                    return DB.query('SHOW FIELD KEYS').then((json) => {
                        var fields = _.flatten(json.results[0].series[0].values);
                        expect(fields).to.include('str');
                    });
                }, retry_options);
            });
        });

        it('intFields override', () => {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"int_value":1,"name":"cpu"}] | write influx -db "test" -intFields "int_value"'
            }).then((res) => {
                return retry(() => {
                    return DB.query('SELECT * FROM cpu WHERE int_value = 1').then((json) => {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(1);
                        expect(data.values[0][3]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('can use name from the point', () => {
            return check_juttle({
                program: 'emit -points [{"m":"cpu","host":"host0","value":0}] | write influx -db "test" -nameField "m"'
            }).then((res) => {
                return retry(() => {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then((json) => {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('by default uses name field for name from the point', () => {
            return check_juttle({
                program: 'emit -points [{"name":"cpu","host":"host0","value":0}] | write influx -db "test"'
            }).then((res) => {
                return retry(() => {
                    return DB.query('SELECT * FROM cpu WHERE value = 0').then((json) => {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host0");
                        expect(data.values[0][2]).to.equal(0);
                    });
                }, retry_options);
            });
        });

        it('two points', () => {
            // This is a workaround for emit adding same time in ms to both points
            // and influx treating time as primary index, overwriting the points
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"time"::now:},{"host":"host1","value":1,"time"::1s ago:}] | put name = "cpu" | write influx -db "test"'
            }).then((res) => {
                return retry(() => {
                    return DB.query('SELECT * FROM cpu').then((json) => {
                        var data = json.results[0].series[0];
                        expect(data.values.length).to.equal(2);
                    });
                }, retry_options);
            });
        });

        it('emits a warning on serialization but continues', () => {
            return check_juttle({
                program: 'emit -points [{"host":"host0","value":0,"time"::0:},{"n":"cpu","host":"host1","value":1,"time"::1:}] | write influx -db "test" -nameField "n"'
            }).then((res) => {
                expect(res.warnings.length).to.equal(1);
                expect(res.warnings[0]).to.include('point is missing a name');
                return retry(() => {
                    return DB.query('SELECT * FROM cpu WHERE value = 1').then((json) => {
                        var data = json.results[0].series[0];
                        expect(data.values[0][1]).to.equal("host1");
                        expect(data.values[0][2]).to.equal(1);
                    });
                }, retry_options);
            });
        });
    });
});
