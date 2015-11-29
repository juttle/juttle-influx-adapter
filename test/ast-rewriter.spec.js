var Rewriter = require('../lib/ast-rewriter');
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

    var new_ast = rewriter.visit(from_ast);
    expect(new_ast).to.deep.equal(to_ast, to);
};

describe('rewriter', function() {
    it.skip('handles key1 in []', function() {
        var tests = ['key1 in []', 'key1 in []'];
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

    it.skip('propagates negation into expressions', function() {
    });
});
