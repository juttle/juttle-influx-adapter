var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var g2r = require('glob-to-regexp');
var ast_utils = require('../ast/utils');

var FromBuilder = ASTVisitor.extend({
    build: function(ast, options) {
        return this.visit(ast, options);
    },

    visitBinaryExpression: function(node, options) {
        if (ast_utils.isNameField(node.left, options.nameField)) {
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
        var left  = this.visit(node.left, options);
        var right = this.visit(node.right, options);
        return left || right;
    },

    visitFilterLiteral: function(node, options) {
        return this.visit(node.ast, options);
    },

    visitSimpleFilterTerm: function(node, options) {
        return this.visit(node.expression, options);
    },

    visitExpressionFilterTerm: function(node, options) {
        return this.visit(node.expression, options);
    },

    visit: function(node, options) {
        return this['visit' + node.type].apply(this, arguments);
    }
});

module.exports = FromBuilder;
