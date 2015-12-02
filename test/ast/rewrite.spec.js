var Rewriter = require('../../lib/ast/rewrite');

var parser = require('juttle/lib/parser');
var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');
var expect = require('chai').expect;

var StripTextAndLocation = ASTVisitor.extend({
    visit: function(node) {
        delete(node.location);
        delete(node.text);
        this['visit' + node.type].apply(this, arguments);
        return node;
    }
});

var check_rewrite = function(from, to) {
    var strip = new StripTextAndLocation();
    var rewriter = new Rewriter();

    var from_ast = strip.visit(parser.parseFilter(from));
    var to_ast = strip.visit(parser.parseFilter(to));

    var new_ast = rewriter.rewrite(from_ast);
    expect(new_ast).to.deep.equal(to_ast, from + ' -> ' + to);
};

describe('rewriter', function() {
    it('handles key1 in []', function() {
        var tests = [
            ['key1 in []', 'false'],
            ['key1 in [] and key1 = "val1"', 'false and key1 = "val1"']
        ];
        _.each(tests, function(test) { check_rewrite(test[0], test[1]); });
    });

    it('rewrites in to or', function() {
        var tests = [
            ['key1 in ["val1"]', 'key1 = "val1"'],
            ['key1 in ["val1", "val2"]', 'key1 = "val1" or key1 = "val2"'],
            ['key1 in ["val1", "val2", "val3"]','key1 = "val1" or key1 = "val2" or key1 = "val3"'],
            ['key1 in ["val1", "val2", "val3", "val4"]','key1 = "val1" or key1 = "val2" or key1 = "val3" or key1 = "val4"'],
            ['key1 in ["val1", "val2"] and key2 in ["val3", "val4"]','( key1 = "val1" or key1 = "val2") and (key2 = "val3" or key2 = "val4")'],
        ];

        _.each(tests, function(test) { check_rewrite(test[0], test[1]); });
    });

    it('propagates negation into expressions', function() {
        var tests = [
            // Binary negations
            ['not (key1 = "val1")', 'key1 != "val1"'],
            ['not (key1 > "val1")', 'key1 <= "val1"'],
            ['not (key1 =~ "val1")', 'key1 !~ "val1"'],
            ['not (key1 !~ "val1")', 'key1 =~ "val1"'],

            // De morgan
            ['not (key1 = "val1" and key1 = "val2")', '(key1 != "val1" or key1 != "val2")'],
            ['not (key1 = "val1" or key1 = "val2")', '(key1 != "val1" and key1 != "val2")'],
            ['not (key1 = "val1" or key1 = "val2" or key1 = "val3")', '(key1 != "val1" and key1 != "val2" and key1 != "val3")'],

            // Nested propagation
            ['not (key1 = "val1" and (key1 = "val2" or key1 = "val3"))', 'key1 != "val1" or (key1 != "val2" and key1 != "val3")'],

            // Double negation
            ['not ( not (key1 = "val1") )', 'key1 = "val1"'],
            ['not ( not (key1 > "val1") )', 'key1 > "val1"'],

            // Funky
            ['not ( ( not (key1 = "val1") ) and ( not ( key1 = "val2" ) ) )', '(key1 == "val1") or (key1 == "val2")']
        ];

        _.each(tests, function(test) { check_rewrite(test[0], test[1]); });
    });
});
