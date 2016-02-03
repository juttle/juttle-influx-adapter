'use strict';

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var g2r = require('glob-to-regexp');
var ast_utils = require('../ast/utils');

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

    build: function(ast, options) {
        var op, where;

        if (!options.nameField) { throw new Error('Missing required option: nameField'); }

        where = this.visit(ast, op, options);

        if (typeof where === 'undefined') {
            return '';
        } else {
            return String(where);
        }
    },

    visitSimpleFilterTerm: function(node, op, options) {
        return this.visit(node.expression, op, options);
    },

    visitBooleanLiteral: function(node, op, options) {
        return node.value;
    },

    visitFilterLiteral: function(node, op, options) {
        return this.visit(node.ast, op, options);
    },

    visitExpressionFilterTerm: function(node, op, options) {
        return this.visit(node.expression, op, options);
    },

    visitRegularExpressionLiteral: function(node, op, options) {
        return '/' + node.value + '/';
    },

    visitStringLiteral: function(node, op, options) {
        if (op && (op === '!~' || op === '=~')) {
            return String(g2r(node.value));
        } else {
            return '\'' + node.value + '\'';
        }
    },

    visitNumericLiteral: function(node, op, options) {
        return node.value;
    },

    visitNullLiteral: function(node, op, options) {
        return 'null';
    },

    visitVariable: function(node, op, options) {
        return '"' + node.name + '"';
    },

    visitUnaryExpression: function(node, op, options) {
        // Field dereference
        if (node.operator === '*') {
            return '"' + node.expression.value + '"';
        } else {
            var operator = this.ops[node.operator];

            if (!operator) { throw new Error('Operator ' + node.operator + ' not supported by InfluxQL'); }

            return '( ' + operator + ' ' + this.visit(node.expression, operator, options) + ' )';
        }
    },

    visitBinaryExpression: function(node, op, options) {
        var operator = this.ops[node.operator];

        if (!operator) { throw new Error('Operator ' + node.operator + ' not supported by InfluxQL'); }

        // Skip nameField, will be part of from
        if (ast_utils.isNameField(node.left, options.nameField)) {
            return;
        }

        var str = "";

        var left = this.visit(node.left, operator, options);
        var right = this.visit(node.right, operator, options);

        if (typeof left !== 'undefined') {
            str += (node.left.type === 'BinaryExpression') ? '(' + left  + ')' : left;
        }
        if (typeof left !== 'undefined' && typeof right !== 'undefined') {
            str += ' ' + operator + ' ';
        }
        if (typeof right !== 'undefined') {
            str += (node.right.type === 'BinaryExpression') ? '(' + right + ')' : right;
        }

        return str;
    },

    visit: function(node, op, options) {
        return this['visit' + node.type].apply(this, arguments);
    }
});

module.exports = WhereBuilder;
