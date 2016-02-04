'use strict';

var expect = require('chai').expect;
var QueryBuilder = require('../../lib/query');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var utils = require('../test_utils');

describe('influxql query building', () => {
    var builder = new QueryBuilder();

    describe('SELECT', () => {
        it('basic query', () => {
            expect(builder.build({ nameField: 'name' })).to.equal('SELECT * FROM /.*/');
        });

        it('with field', () => {
            expect(builder.build({ nameField: 'name', fields: 'key1' })).to.equal('SELECT key1 FROM /.*/');
        });

        it('with fields', () => {
            expect(builder.build({ nameField: 'name', fields: ['key1', 'key2'] })).to.equal('SELECT key1,key2 FROM /.*/');
        });

        it('with regexp', () => {
            expect(builder.build({ nameField: 'name', fields: ['key1', 'key2'] })).to.equal('SELECT key1,key2 FROM /.*/');
        });

        describe('FROM', () => {
            it('defaults to /.*/', () => {
                expect(builder.build({ nameField: 'name' }, {})).to.equal('SELECT * FROM /.*/');
            });

            it('uses value of name', () => {
                var ast = utils.parseFilter('name = "cpu"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM "cpu"');
            });

            it('name accepts globs', () => {
                var ast = utils.parseFilter('name =~ "cpu"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /^cpu$/');
            });

            it('name accepts regexps', () => {
                var ast = utils.parseFilter('name =~ /cpu/');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /cpu/');
            });

            it('can be changed using nameField option', () => {
                var ast = utils.parseFilter('metric =~ /cpu/');
                expect(builder.build({ nameField: 'metric' }, { filter_ast: ast })).to.equal('SELECT * FROM /cpu/');
            });
        });

        describe('LIMIT', () => {
            it('adds clause', () => {
                expect(builder.build({ nameField: 'name', limit: 1 })).to.equal('SELECT * FROM /.*/ LIMIT 1');
            });
        });

        describe('OFFSET', () => {
            it('adds clause', () => {
                expect(builder.build({ nameField: 'name', offset: 1 })).to.equal('SELECT * FROM /.*/ OFFSET 1');
            });

            it('correct order with limit', () => {
                expect(builder.build({ nameField: 'name', offset: 1, limit: 2 })).to.equal('SELECT * FROM /.*/ LIMIT 2 OFFSET 1');
            });
        });

        describe('ORDER BY', () => {
            it('adds clause', () => {
                expect(builder.build({ orderBy: 'time', nameField: 'name' })).to.equal('SELECT * FROM /.*/ ORDER BY time ASC');
            });

            it('adds clause for direction', () => {
                expect(builder.build({ orderBy: 'time', orderDescending: true, nameField: 'name' })).to.equal('SELECT * FROM /.*/ ORDER BY time DESC');
            });

            it('correct order with limit', () => {
                expect(builder.build({ offset: 1, limit: 2, orderBy: 'time', nameField: 'name' })).to.equal('SELECT * FROM /.*/ ORDER BY time ASC LIMIT 2 OFFSET 1');
            });
        });

        describe('WHERE', () => {
            it('simple filter', () => {
                var ast = utils.parseFilter('key = 1');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = 1');
            });

            it('simple filter name used as from', () => {
                var ast = utils.parseFilter('name = "test"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM "test"');
            });

            it('name in implicit and is used as from', () => {
                var ast = utils.parseFilter('name = "test" key = "val"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM "test" WHERE "key" = \'val\'');
            });

            it('simple filter string', () => {
                var ast = utils.parseFilter('key = "val"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = \'val\'');
            });

            it('simple filter number', () => {
                var ast = utils.parseFilter('key = 1');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = 1');
            });

            it('simple filter null', () => {
                var ast = utils.parseFilter('key = null');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = null');
            });

            it('implicit and', () => {
                var ast = utils.parseFilter('key1 = "val1" key2 = "val2"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\' AND "key2" = \'val2\'');
            });

            it('and, nested or', () => {
                var ast = utils.parseFilter('key1 = "val1" AND (key2 = "val2" OR key3 = "val3")');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\' AND ("key2" = \'val2\' OR "key3" = \'val3\')');
            });

            it('regular expressions', () => {
                var ast = utils.parseFilter('key1 =~ /val1/');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" =~ /val1/');
            });

            it('regular expression negations', () => {
                var ast = utils.parseFilter('key1 !~ /val1/');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" !~ /val1/');
            });

            it('treats globs as regexes', () => {
                var ast = utils.parseFilter('key1 =~ "t*est"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" =~ /^t.*est$/');
            });

            it('in operator is converted to or sequence', () => {
                var ast1 = utils.parseFilter('key1 in ["val1"]');
                var ast2 = utils.parseFilter('key1 in ["val1", "val2"]');
                var ast3 = utils.parseFilter('key1 in ["val1", "val2", "val3"]');

                expect(builder.build({ nameField: 'name' }, { filter_ast: ast1 })).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\'');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast2 })).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\' OR "key1" = \'val2\'');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast3 })).to.equal('SELECT * FROM /.*/ WHERE ("key1" = \'val1\' OR "key1" = \'val2\') OR "key1" = \'val3\'');
            });

            it('in operator numeric', () => {
                var ast = utils.parseFilter('key1 in [1, 2]');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" = 1 OR "key1" = 2');
            });

            it('in operator with empty array is handled', () => {
                var ast0 = utils.parseFilter('key1 in []');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast0 })).to.equal('SELECT * FROM /.*/ WHERE false');
            });

            it('handles compound ops', () => {
                var ast0 = utils.parseFilter('key1 = "val1" and key1 = "val2" or key1 = "val3"');
                expect(builder.build({ nameField: 'name' }, { filter_ast: ast0 })).to.equal('SELECT * FROM /.*/ WHERE ("key1" = \'val1\' AND "key1" = \'val2\') OR "key1" = \'val3\'');
            });

            describe('NOT', () => {
                it('in', () => {
                    var ast = utils.parseFilter('not (key1 in ["val1", "val2"])');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" != \'val1\' AND "key1" != \'val2\'');
                });

                it('>', () => {
                    var ast = utils.parseFilter('not (key1 > 1)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" <= 1');
                });

                it('<', () => {
                    var ast = utils.parseFilter('not (key1 < 1)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" >= 1');
                });

                it('=', () => {
                    var ast = utils.parseFilter('not (key1 = 1)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1');
                });

                it('=~', () => {
                    var ast = utils.parseFilter('not (key1 =~ /val1/)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" !~ /val1/');
                });

                it('AND implicit', () => {
                    var ast = utils.parseFilter('not (key1 = 1 key2 = 2)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1 OR "key2" != 2');
                });

                it('AND explicit', () => {
                    var ast = utils.parseFilter('not (key1 = 1 and key2 = 2)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1 OR "key2" != 2');
                });

                it('AND explicit inequalities', () => {
                    var ast = utils.parseFilter('not (key1 < 1 and key2 > 2)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" >= 1 OR "key2" <= 2');
                });

                it('OR', () => {
                    var ast = utils.parseFilter('not (key1 = 1 OR key2 = 2)');
                    expect(builder.build({ nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1 AND "key2" != 2');
                });
            });

            describe('time in filters', () => {
                it('is not allowed as time field', () => {
                    var ast = utils.parseFilter('time > 1449229920');
                    expect(builder.build.bind(builder, { nameField: 'name' }, { filter_ast: ast })).to.throw(/Time field is not allowed in filter/);
                });

                it('is not allowed as moment', () => {
                    var ast = utils.parseFilter('created_at > :now:');
                    expect(builder.build.bind(builder, { nameField: 'name' }, { filter_ast: ast })).to.throw(/Filtering by time is not supported/);
                });
            });

            describe('to and from', () => {
                var t1 = new Date(Date.now() - 3600 * 1000);
                var t2 = new Date(Date.now());

                var from = new JuttleMoment(t1);
                var to = new JuttleMoment(t2);

                it('is added into the query', () => {
                    var ast = utils.parseFilter('key = 1');
                    expect(builder.build({ from: from, to: to, nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = 1 AND "time" >= \'' + from.valueOf() + '\' AND "time" < \'' + to.valueOf() + '\'');
                });

                it('is added to empty filter', () => {
                    expect(builder.build({ from: from, to: to, nameField: 'name' })).to.equal('SELECT * FROM /.*/ WHERE "time" >= \'' + from.valueOf() + '\' AND "time" < \'' + to.valueOf() + '\'');
                });

                it('is added to binary expr', () => {
                    var ast = utils.parseFilter('key = 1 and key = 2');
                    expect(builder.build({ from: from, to: to, nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = 1 AND "key" = 2 AND "time" >= \'' + from.valueOf() + '\' AND "time" < \'' + to.valueOf() + '\'');
                });

                it('is added to unary expr', () => {
                    var ast = utils.parseFilter('not (key = 1)');
                    expect(builder.build({ from: from, to: to, nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" != 1 AND "time" >= \'' + from.valueOf() + '\' AND "time" < \'' + to.valueOf() + '\'');
                });

                it('with name field', () => {
                    var ast = utils.parseFilter('name = "cpu"');
                    expect(builder.build({ from: from, to: to, nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM "cpu" WHERE "time" >= \'' + from.valueOf() + '\' AND "time" < \'' + to.valueOf() + '\'');
                });

                it('only from', () => {
                    expect(builder.build({ from: from, nameField: 'name' })).to.equal('SELECT * FROM /.*/ WHERE "time" >= \'' + from.valueOf() + '\'');
                });

                it('only from and filter', () => {
                    var ast = utils.parseFilter('key = 1');
                    expect(builder.build({ from: from, nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = 1 AND "time" >= \'' + from.valueOf() + '\'');
                });

                it('only to', () => {
                    expect(builder.build({ to: to, nameField: 'name' })).to.equal('SELECT * FROM /.*/ WHERE "time" < \'' + to.valueOf() + '\'');
                });

                it('only to and filter', () => {
                    var ast = utils.parseFilter('key = 1');
                    expect(builder.build({ to: to, nameField: 'name' }, { filter_ast: ast })).to.equal('SELECT * FROM /.*/ WHERE "key" = 1 AND "time" < \'' + to.valueOf() + '\'');
                });
            });
        });
    });
});
