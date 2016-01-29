'use strict';

var expect = require('chai').expect;
var _ = require('underscore');
var utils = require('../test_utils');

var Linter = require('../../lib/ast/lint');

describe('query linting', function() {
    var linter = new Linter();

    it('doesnt allow to specify name more than once', function() {
        var tests = [
            'name = "cpu" and name = "hi"',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' })).to.throw(/Name field name can be present at most once/);
        });
    });

    it('validates field according to nameField option', function() {
        var tests = [
            'metric = "cpu" and metric = "hi"',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'metric' })).to.throw(/Name field metric can be present at most once/);
        });
    });

    it('name can be filtered using equality or regex', function() {
        var tests = [
            'name == "cpu"',
            'name = "cpu"',
            'name =~ "cpu"',
            'name =~ /cpu/',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.not.throw;
        });
    });

    it('name can be only part of a regex or string equality comparison', function() {
        var tests = [
            'name !~ "cpu"',
            'name != "cpu"',
            'name in ["a","b","c"]',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.throw(/Only equality and regular expressions are supported/);
        });
    });

    it('name cannot be empty', function() {
        var tests = [
            'name =~ ""',
            'name == ""',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.throw(/nameField filter cannot be empty/);
        });
    });

    it('doesnt allow name to be a part of unary expression', function() {
        var tests = [
            'not (name == "cpu")',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.throw(/name cannot be nested inside the filter/);
        });
    });

    it('name filter rhs must be a string or a regex', function() {
        var tests = [
            'name = 1',
            'name = Date.new(0)'
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.throw(/nameField filter must be a string or a regexp/);
        });
    });

    it('allows for name to be a part of binary expression with and', function() {
        var tests = [
            'name == "cpu" and field1 == "val1"',
            '(name == "cpu" and field1 == "val2") and (field2 == "val2")',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.not.throw;
        });
    });

    it('doesnt allow name to be a part of binary expression with or', function() {
        var tests = [
            'name == "cpu" or field1 == "val1"',
            '(name == "cpu" and field1 == "val2") or (field2 == "val2")',
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' }), test).to.throw(/name cannot be nested inside the filter/);
        });
    });

    it('doesnt allow time field on LHS', function() {
        var tests = [
            '(time in [])',
            'time > :now:',
            'not (time > :2 days ago:)',
            'time = :2014-01-01:'
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' })).to.throw(/Time field is not allowed in filter expressions/);
        });
    });

    it('doesnt allow time / duration expressions in filters', function() {
        var tests = [
            'field > :now:',
            'field > :5m:',
            'not (field > :2 days ago:)',
            'field = :2014-01-01:'
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' })).to.throw(/Filtering by time is not supported/);
        });
    });

    it('doesnt allow forever/beginning/end in filters', function() {
        var tests = [
            'field > :beginning:',
            'not (field > :end:)',
            'field = :forever:'
        ];

        _.each(tests, function(test) {
            var ast = utils.parseFilter(test);
            expect(linter.lint.bind(linter, ast, { nameField: 'name' })).to.throw(/Filtering by time is not supported/);
        });
    });
});
