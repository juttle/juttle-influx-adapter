var expect = require('chai').expect;
var parser = require('juttle/lib/parser');
var QueryBuilder = require('../../lib/query');

describe('influxql query building', function() {
    var builder = new QueryBuilder();

    describe('SELECT', function() {
        it('basic query', function() {
            expect(builder.build()).to.equal('SELECT * FROM /.*/');
        });

        it('with field', function() {
            expect(builder.build({fields: 'key1'})).to.equal('SELECT key1 FROM /.*/');
        });

        it('with fields', function() {
            expect(builder.build({fields: ['key1', 'key2']})).to.equal('SELECT key1,key2 FROM /.*/');
        });

        it('with measurement', function() {
            expect(builder.build({measurements: 'm'})).to.equal('SELECT * FROM m');
        });

        it('with measuremlnts', function() {
            expect(builder.build({measurements: ['m1', 'm2']})).to.equal('SELECT * FROM m1,m2');
        });

        it('with regexp', function() {
            expect(builder.build({fields: ['key1', 'key2']})).to.equal('SELECT key1,key2 FROM /.*/');
        });

        describe('LIMIT', function() {
            it('adds clause', function() {
                expect(builder.build({limit: 1})).to.equal('SELECT * FROM /.*/ LIMIT 1');
            });
        });

        describe('OFFSET', function() {
            it('adds clause', function() {
                expect(builder.build({offset: 1})).to.equal('SELECT * FROM /.*/ OFFSET 1');
            });

            it('correct order with limit', function() {
                expect(builder.build({offset: 1, limit: 2})).to.equal('SELECT * FROM /.*/ LIMIT 2 OFFSET 1');
            });
        });

        describe('WHERE', function() {
            it('simple filter', function() {
                var ast = parser.parseFilter('key = 1');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key" = 1');
            });

            it('simple filter string', function() {
                var ast = parser.parseFilter('key = "val"');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key" = \'val\'');
            });

            it('implicit and', function() {
                var ast = parser.parseFilter('key1 = "val1" key2 = "val2"');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\' AND "key2" = \'val2\'');
            });

            it('and, nested or', function() {
                var ast = parser.parseFilter('key1 = "val1" AND (key2 = "val2" OR key3 = "val3")');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\' AND ("key2" = \'val2\' OR "key3" = \'val3\')');
            });

            it('regular expressions', function() {
                var ast = parser.parseFilter('key1 =~ /val1/');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" =~ /val1/');
            });

            it('regular expression negations', function() {
                var ast = parser.parseFilter('key1 !~ /val1/');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" !~ /val1/');
            });

            it('treats globs as regexes', function() {
                var ast = parser.parseFilter('key1 =~ "t*est"');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" =~ /^t.*est$/');
            });

            it('in operator is converted to or sequence', function() {
                var ast1 = parser.parseFilter('key1 in ["val1"]');
                var ast2 = parser.parseFilter('key1 in ["val1", "val2"]');
                var ast3 = parser.parseFilter('key1 in ["val1", "val2", "val3"]');

                expect(builder.build({}, {filter_ast: ast1})).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\'');
                expect(builder.build({}, {filter_ast: ast2})).to.equal('SELECT * FROM /.*/ WHERE "key1" = \'val1\' OR "key1" = \'val2\'');
                expect(builder.build({}, {filter_ast: ast3})).to.equal('SELECT * FROM /.*/ WHERE ("key1" = \'val1\' OR "key1" = \'val2\') OR "key1" = \'val3\'');
            });

            it('in operator numeric', function() {
                var ast = parser.parseFilter('key1 in [1, 2]');
                expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" = 1 OR "key1" = 2');
            });

            it('in operator with empty array is handled', function() {
                var ast0 = parser.parseFilter('key1 in []');
                expect(builder.build({}, {filter_ast: ast0})).to.equal('SELECT * FROM /.*/ WHERE false');
            });

            it('handles compound ops', function() {
                var ast0 = parser.parseFilter('key1 = "val1" and key1 = "val2" or key1 = "val3"');
                expect(builder.build({}, {filter_ast: ast0})).to.equal('SELECT * FROM /.*/ WHERE ("key1" = \'val1\' AND "key1" = \'val2\') OR "key1" = \'val3\'');
            });

            describe('NOT', function() {
                it('in', function() {
                    var ast = parser.parseFilter('not (key1 in ["val1", "val2"])');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" != \'val1\' AND "key1" != \'val2\'');
                });

                it('>', function() {
                    var ast = parser.parseFilter('not (key1 > 1)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" <= 1');
                });

                it('<', function() {
                    var ast = parser.parseFilter('not (key1 < 1)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" >= 1');
                });

                it('=', function() {
                    var ast = parser.parseFilter('not (key1 = 1)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1');
                });

                it('=~', function() {
                    var ast = parser.parseFilter('not (key1 =~ /val1/)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" !~ /val1/');
                });

                it('AND implicit', function() {
                    var ast = parser.parseFilter('not (key1 = 1 key2 = 2)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1 OR "key2" != 2');
                });

                it('AND explicit', function() {
                    var ast = parser.parseFilter('not (key1 = 1 and key2 = 2)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1 OR "key2" != 2');
                });

                it('AND explicit inequalities', function() {
                    var ast = parser.parseFilter('not (key1 < 1 and key2 > 2)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" >= 1 OR "key2" <= 2');
                });

                it('OR', function() {
                    var ast = parser.parseFilter('not (key1 = 1 OR key2 = 2)');
                    expect(builder.build({}, {filter_ast: ast})).to.equal('SELECT * FROM /.*/ WHERE "key1" != 1 AND "key2" != 2');
                });
            });
        });
    });
});