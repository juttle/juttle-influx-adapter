'use strict';

/*
 * Rewrites juttle in operator into a sequence of ors (degenerating into a
 * simple equality for one element array and into 'false' for an empty one.
 */

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');

var Rewriter = ASTVisitor.extend({
    _hasIn: function(node) {
        return (node.type === 'ExpressionFilterTerm' &&
                node.expression.type === 'BinaryExpression' &&
                node.expression.operator === 'in');
    },

    _rewriteIn: function(_node) {
        var elements = _node.expression.right.elements;

        var equalities = _.map(elements, function(elm) {
            return {
                type: 'ExpressionFilterTerm',
                expression: {
                    type: 'BinaryExpression',
                    operator: '==',
                    left: _node.expression.left,
                    right: elm,
                }
            };
        });

        if (equalities.length === 0) {
            return {
                type: 'SimpleFilterTerm',
                expression: {
                    type: 'BooleanLiteral',
                    value: false
                }
            };
        } else {
            var ors = _.reduce(equalities, function(or, elm) {
                return {
                    type: 'BinaryExpression',
                    operator: 'OR',
                    left: or,
                    right: elm
                };
            }, equalities.shift());
            return ors;
        }
    },

    visitFilterLiteral: function(node) {
        if (this._hasIn(node.ast)) { node.ast = this._rewriteIn(node.ast); }
        this.visit(node.ast);
    },

    visitUnaryExpression: function(node) {
        if (this._hasIn(node.expression)) { node.expression = this._rewriteIn(node.expression); }
        this.visit(node.expression);
    },

    visitBinaryExpression: function(node) {
        if (this._hasIn(node.right)) { node.right = this._rewriteIn(node.right); }
        if (this._hasIn(node.left)) { node.left = this._rewriteIn(node.left); }

        this.visit(node.left);
        this.visit(node.right);
    },

    visit: function(node) {
        this['visit' + node.type].apply(this, arguments);
    }
});

module.exports = Rewriter;
