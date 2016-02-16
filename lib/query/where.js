'use strict';

/* global JuttleAdapterAPI */
var StaticFilterCompiler = JuttleAdapterAPI.compiler.StaticFilterCompiler;
var g2r = require('glob-to-regexp');
var ast_utils = require('../ast/utils');

class WhereBuilder extends StaticFilterCompiler {
    constructor(adapterName) {
        super(adapterName);

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
    }

    build(root, options) {
        var op, where;

        if (!options.nameField) { throw new Error('Missing required option: nameField'); }

        where = this.compile(root, op, options);

        if (typeof where === 'undefined') {
            return '';
        } else {
            return String(where);
        }
    }

    compileLiteral(node, op, options) {
        switch (node.type) {
            case 'BooleanLiteral':
            case 'NumericLiteral':
                return node.value;
            case 'RegularExpressionLiteral':
                return `/${node.pattern}/`;
            case 'StringLiteral':
                if (op && (op === '!~' || op === '=~')) {
                    return String(g2r(node.value));
                } else {
                    return `'${node.value}'`;
                }
            default:
                super.compileLiteral(node, op, options);
        }
    }

    compileField(node, op, options) {
        return `"${node.name}"`;
    }

    compileExpressionTerm(node, op, options) {
        var operator = this.ops[node.operator];

        if (!operator) { throw new Error(`Operator ${node.operator} not supported by InfluxQL`); }

        if (ast_utils.isNameField(node.left, options.nameField)) {
            return;
        }

        var left = this.compile(node.left, operator, options);
        var right = this.compile(node.right, operator, options);

        return `${left} ${operator} ${right}`;
    }

    compileBooleanExpression(node, op, options) {
        var left = this.compile(node.left, op, options);
        var right = this.compile(node.right, op, options);
        var str = "";

        if (left !== undefined) {
            str += (node.left.type === 'BinaryExpression') ? `(${left})` : left;
        }
        if (left !== undefined && right !== undefined) {
            str += ` ${op} `;
        }
        if (right !== undefined) {
            str += (node.right.type === 'BinaryExpression') ? `(${right})` : right;
        }

        return str;
    }

    compileAndExpression(node, op, options) {
        return this.compileBooleanExpression(node, 'AND' , options);
    }

    compileOrExpression(node, op, options) {
        return this.compileBooleanExpression(node, 'OR', options);
    }
}

module.exports = WhereBuilder;
