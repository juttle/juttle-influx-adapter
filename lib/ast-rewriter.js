/*
 * Rewrites juttle specific subtrees and operators so that they're influx-db
 * compatible. This simplifies building the actual where filters.
 *
 * Currently, these rewrites are taking place:
 *
 *   * Inclusion operator ('in') is rewrittten to a sequence of ORs.
 *   * FIXME: Propagates unary negation into expressions
 */

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');

var ASTRewriter = ASTVisitor.extend({
    _hasIn: function(node) {
        return (node.type === 'ExpressionFilterTerm' &&
                node.expression.type === 'BinaryExpression' &&
                node.expression.operator === 'in');
    },

    _rewriteIn: function(_node) {
        var node = JSON.parse(JSON.stringify(_node));
        var elements = node.expression.right.elements;

        var equalities = _.map(elements, function(elm) {
            return {
                type: 'ExpressionFilterTerm',
                expression: {
                    type: 'BinaryExpression',
                    operator: '==',
                    left: node.expression.left,
                    right: elm,
                }
            };
        });

        if (equalities.length === 0) {
            return node;
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

        return node;
    },

    visitBinaryExpression: function(node) {
        if (this._hasIn(node.right)) { node.right = this._rewriteIn(node.right); }
        if (this._hasIn(node.left)) { node.left = this._rewriteIn(node.left); }

        this.visit(node.left);
        this.visit(node.right);

        return node;
    },

    visit: function(node) {
        this['visit' + node.type].apply(this, arguments);
        return node;
    }
});

module.exports = ASTRewriter;
