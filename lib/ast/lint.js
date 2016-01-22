'use strict';

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');

var Lints = {
    lint: function(ast) {
        var options = {
            names: 0,
            disallow_name: false
        };

        this.visit(ast, options);

        if (options.names > 1) {
            throw new Error('Name field can be present only once');
        }
    },

    visitBinaryExpression: function(node, options) {
        if (node.left.name === 'name') {
            if (node.operator !== '==' && node.operator !== '=~') {
                throw new Error('Only equality and regular expressions are supported for name field');
            }

            if (node.right.type !== 'StringLiteral' && node.right.type !== 'RegularExpressionLiteral') {
                throw new Error('Name filter must be a string or a regexp');
            }

            if (node.right.type === 'StringLiteral' || node.right.type === 'RegularExpressionLiteral') {
                if (node.right.value === '') {
                    throw new Error('Name filter cannot be empty');
                }
            }
        }

        if (node.operator === 'OR') {
            options.disallow_name = true;
        };

        this.visit(node.left, options);
        this.visit(node.right, options);

        options.disallow_name = false;
    },

    visitVariable: function(node, options) {
        // No time on LHS.
        if (node.name === "time") {
            throw new Error('Time field is not allowed in filter expressions, use -from and/or -to');
        }

        // Count names
        if (node.name === 'name') {
            options.names += 1;
        }
    },

    visitUnaryExpression: function(node, options) {
        options.disallow_name = true;
        this.visit(node.expression, options);
        options.disallow_name = false;
    },

    visit: function(node, options) {
        if (node.name === 'name' && options.disallow_name) {
            throw new Error('Name cannot be nested inside the filter.');
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
