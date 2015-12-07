var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var g2r = require('glob-to-regexp');

var WhereBuilder = ASTVisitor.extend({
    initialize: function() {
        this.ops = {
                '==': '=',
                'AND': 'AND',
                'OR': 'OR',
                '!=': '!=',
                '<' : '<',
                '>' : '>',
                '>=' : '>=',
                '<=' : '<=',
                '=~' : '=~',
                '!~' : '!~',
                //'NOT': undefined,
                //'IN': undefined,
        };
    },

    build: function(ast) {
        return this.visit(ast);
    },

    visitSimpleFilterTerm: function(node) {
        return this.visit(node.expression);
    },

    visitBooleanLiteral: function(node) {
        return node.value;
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

    visitNullLiteral: function(node) {
        return 'null';
    },

    visitVariable: function(node) {
        return '"' + node.name + '"';
    },

    visitUnaryExpression: function(node) {
        // Field dereference
        if (node.operator === '*') {
            return '"' + node.expression.value + '"';
        } else {
            var op = this.ops[node.operator];

            if (!op) { throw new Error('Operator ' + node.operator + ' not supported by InfluxQL'); }

            return '( ' + op + ' ' + this.visit(node.expression) + ' )';
        }
    },

    visitBinaryExpression: function(node) {
        var op = this.ops[node.operator];

        if (!op) { throw new Error('Operator ' + node.operator + ' not supported by InfluxQL'); }

        var str = "";

        var left = this.visit(node.left, op);
        var right = this.visit(node.right, op);

        str += (node.left.type === 'BinaryExpression') ? '(' + left  + ')' : left;
        str += ' ' + op + ' ';
        str += (node.right.type === 'BinaryExpression') ? '(' + right + ')' : right;

        return str;
    }
});

module.exports = WhereBuilder;
