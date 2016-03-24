'use strict';

/* global JuttleAdapterAPI */
var ASTVisitor = JuttleAdapterAPI.compiler.ASTVisitor;
var ast_utils = require('./utils');

class Linter extends ASTVisitor {
    lint(ast, o) {
        if (!o.nameField) { throw new Error('Missing required option: nameField'); }

        var options = {
            nameFilters: 0,
            disallowNameField: false,
            nameField: o.nameField
        };

        this.visit(ast, options);

        if (options.nameFilters > 1) {
            throw new Error(`Name field ${options.nameField} can be present at most once`);
        }
    }

    visitBinaryExpression(node, options) {
        if (ast_utils.isNameField(node.left, options.nameField)) {
            if (node.operator !== '==' && node.operator !== '=~') {
                throw new Error('Only equality and regular expressions are supported for nameField');
            }

            if (node.right.type !== 'StringLiteral' && node.right.type !== 'RegExpLiteral') {
                throw new Error('nameField filter must be a string or a regexp');
            } else {
                if (node.right.value === '') {
                    throw new Error('nameField filter cannot be empty');
                }
            }
        }

        if (node.operator === 'OR') {
            options.disallowNameField = true;
        }

        this.visit(node.left, options);
        this.visit(node.right, options);

        options.disallowNameField = false;
    }

    visitUnaryExpression(node, options) {
        options.disallowNameField = true;
        this.visit(node.argument, options);
        options.disallowNameField = false;
    }

    visitField(node, options) {
        // No time on LHS.
        if (ast_utils.isTime(node)) {
            throw new Error('Time field is not allowed in filter expressions, use -from and/or -to');
        }

        // Count names
        if (ast_utils.isNameField(node, options.nameField)) {
            options.nameFilters += 1;
        }
    }

    visitMemberExpression(node, op, options) {
        throw new Error('Nested fields not supported by InfluxQL');
    }

    visit(node, options) {
        if (ast_utils.isNameField(node, options.nameField) && options.disallowNameField) {
            throw new Error(`${options.nameField} cannot be nested inside the filter.`);
        }
        this[`visit${node.type}`].apply(this, arguments);
    }

    visitNullLiteral(node, options) {
        throw new Error('Filtering null values is not supported');
    }

    visitFulltextFilterTerm(node, options) {
        throw new Error('Full text search is not supported');
    }

    visitMomentLiteral(node, options) {
        throw new Error('Filtering by time is not supported');
    }

    visitDurationLiteral(node, options) {
        throw new Error('Filtering by time is not supported');
    }
}

module.exports = Linter;
