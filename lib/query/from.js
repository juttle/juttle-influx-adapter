var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var g2r = require('glob-to-regexp');

var FromBuilder = ASTVisitor.extend({
    build: function(ast) {
        return this.visit(ast);
    },

    visitBinaryExpression: function(node) {
        if (node.left.type === 'Variable' && node.left.name === 'name') {
            if (node.operator === '=~') {
                if (node.right.type === 'StringLiteral') {
                    return String(g2r(node.right.value));
                } else {
                    return '/' + node.right.value + '/';
                }
            } else {
                return '"' + node.right.value + '"';
            }
        }
        var left  = this.visit(node.left);
        var right = this.visit(node.right);
        return left || right;
    },

    visitFilterLiteral: function(node) {
        return this.visit(node.ast);
    },

    visitSimpleFilterTerm: function(node) {
        return this.visit(node.expression);
    },

    visitExpressionFilterTerm: function(node) {
        return this.visit(node.expression);
    },

    visit: function(node) {
        return this['visit' + node.type].apply(this, arguments);
    }
});

module.exports = FromBuilder;
