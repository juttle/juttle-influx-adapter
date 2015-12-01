var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var g2r = require('glob-to-regexp');

var WhereBuilder = ASTVisitor.extend({
    initialize: function() {
        this.ops = {
                '==': '=',
                'AND': 'AND',
                'OR': 'OR',
                //'NOT': undefined,
                '!=': '!=',
                '<' : '<',
                '>' : '>',
                '>=' : '>=',
                '<=' : '<=',
                '=~' : '=~',
                '!~' : '!~'
        };
    },

    build: function(ast) {
        return this.visit(ast);
    },

    visitFilterLiteral: function(node) {
        return this.visit(node.ast);
    },

    visitExpressionFilterTerm: function(node) {
        return this.visit(node.expression);
    },

    visitRegularExpressionLiteral: function(node) {
        return '/' + node.value + '/';
    },

    visitStringLiteral: function(node, op) {
        if (op && (op === '!~' || op === '=~')) {
            return String(g2r(node.value));
        } else {
            return '\'' + node.value + '\'';
        }
    },

    visitNumericLiteral: function(node) {
        return node.value;
    },

    visitVariable: function(node) {
        return '"' + node.name + '"';
    },

    visitUnaryExpression: function(node) {
        // Field dereference
        if (node.operator === '*') {
            return '"' + node.expression.value + '"';
        } else {
            return '( ' + this.ops[node.operator] + ' ' + this.visit(node.expression) + ' )';
        }
    },

    visitBinaryExpression: function(node) {
        var op = node.operator;
        var str = "";

        // In operator has been rewritten out to either a single equality or
        // disjunctions of equalities. The remaining case is 'key in []' case
        // which is always false in juttle
        if (op === 'in') {
            str += 'false';
        } else {
            var left = this.visit(node.left, op);
            var right = this.visit(node.right, op);

            str += (node.left.type === 'BinaryExpression') ? '(' + left  + ')' : left;
            str += ' ' + (this.ops[op] || '') + ' ';
            str += (node.right.type === 'BinaryExpression') ? '(' + right + ')' : right;
        }

        return str;
    }
});

module.exports = WhereBuilder;
