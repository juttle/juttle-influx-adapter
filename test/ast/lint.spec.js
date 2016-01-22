'use strict';

var _ = require('underscore');

var Linter = require('../../lib/ast/lint');

var expect = require('chai').expect;
var parser = require('juttle/lib/parser');

describe('query linting', function() {
    var linter = new Linter();

    it('doesnt allow time field on LHS', function() {
        var tests = [
            '(time in [])',
            'time > :now:',
            'not (time > :2 days ago:)',
            'time = :2014-01-01:'
        ];

        _.each(tests, function(test) {
            var ast = parser.parseFilter(test);
            expect(linter.lint.bind(linter, ast)).to.throw(/Time field is not allowed in filter expressions/);
        });
    });

    it('doesnt allow time / duration expressions in filters', function() {
        var tests = [
            'field > :now:',
            'not (field > :2 days ago:)',
            'field = :2014-01-01:'
        ];

        _.each(tests, function(test) {
            var ast = parser.parseFilter(test);
            expect(linter.lint.bind(linter, ast)).to.throw(/Filtering by time is not supported/);
        });
    });

    it('doesnt allow forever/beginning/end in filters', function() {
        var tests = [
            'field > :beginning:',
            'not (field > :end:)',
            'field = :forever:'
        ];

        _.each(tests, function(test) {
            var ast = parser.parseFilter(test);
            expect(linter.lint.bind(linter, ast)).to.throw(/Filtering by time is not supported/);
        });
    });
});
