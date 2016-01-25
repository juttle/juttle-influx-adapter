'use strict';

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');

var Lints = {
    lint: function(ast, o) {
        if (!o.nameField) { throw new Error('Missing required option: nameField'); }

        var options = {
            nameFilters: 0,
            disallowNameField: false,
            nameField: o.nameField
        };

        this.visit(ast, options);

        if (options.nameFilters > 1) {
            throw new Error('Name field ' + options.nameField + ' can be present at most once');
        }
    },

    isNameField: function(node, nameField) {
        return node.type === 'UnaryExpression' && node.operator === '*' && node.value === nameField;
    },

    isTime: function(node) {
        return node.type === 'UnaryExpression' && node.operator === '*' && node.value === 'time';
    },

    visitBinaryExpression: function(node, options) {
        if (this.isNameField(node.left, options.nameField)) {
            if (node.operator !== '==' && node.operator !== '=~') {
                throw new Error('Only equality and regular expressions are supported for nameField');
            }

            if (node.right.type !== 'StringLiteral' && node.right.type !== 'RegularExpressionLiteral') {
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
    },

    visitUnaryExpression: function(node, options) {
        // No time on LHS.
        if (this.isTime(node)) {
            throw new Error('Time field is not allowed in filter expressions, use -from and/or -to');
        }

        // Count names
        if (this.isNameField(node, options.nameField)) {
            options.nameFilters += 1;
        }

        options.disallowNameField = true;
        this.visit(node.expression, options);
        options.disallowNameField = false;
    },

    visit: function(node, options) {
        if (this.isNameField(node, options.nameField) && options.disallowNameField) {
            throw new Error(options.nameField + ' cannot be nested inside the filter.');
        }
        this['visit' + node.type].apply(this, arguments);
    }
};

// Duration comparisons are not supported (they could be converted to moments, though)
_.each(['MomentLiteral', 'DurationLiteral'], function(node_name) {
    Lints['visit' + node_name] = function(node, options) {
        throw new Error('Filtering by time is not supported');
    };
});

module.exports = ASTVisitor.extend(Lints);
