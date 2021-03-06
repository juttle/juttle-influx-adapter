'use strict';

/* global JuttleAdapterAPI */
var ASTVisitor = JuttleAdapterAPI.compiler.ASTVisitor;
var g2r = require('glob-to-regexp');
var ast_utils = require('../ast/utils');

class FromBuilder extends ASTVisitor {
    build(ast, options) {
        return this.visit(ast, options);
    }

    visitBinaryExpression(node, options) {
        if (ast_utils.isNameField(node.left, options.nameField)) {
            if (node.operator === '=~') {
                if (node.right.type === 'StringLiteral') {
                    return String(g2r(node.right.value));
                } else {
                    return `/${node.right.pattern}/`;
                }
            } else {
                return `"${node.right.value}"`;
            }
        }
        var left  = this.visit(node.left, options);
        var right = this.visit(node.right, options);
        return left || right;
    }

    visitFilterLiteral(node, options) {
        return this.visit(node.ast, options);
    }

    visit(node, options) {
        return this[`visit${node.type}`].apply(this, arguments);
    }
}

module.exports = FromBuilder;
