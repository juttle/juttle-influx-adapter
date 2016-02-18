'use strict';

/*
 * Rewrites juttle in operator into a sequence of ors (degenerating into a
 * simple equality for one element array and into 'false' for an empty one.
 *
 * Lack of IN operator is tracked in Influx issues
 * https://github.com/influxdata/influxdb/issues/5626
 * https://github.com/influxdata/influxdb/issues/2157
 */

var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');

class Rewriter extends ASTVisitor {
    _hasIn(node) {
        return (node.type === 'BinaryExpression' && node.operator === 'in');
    }

    _rewriteIn(_node) {
        var elements = _node.right.elements;

        var equalities = _.map(elements, (elm) => {
            return {
                type: 'BinaryExpression',
                operator: '==',
                left: _node.left,
                right: elm,
            };
        });

        if (equalities.length === 0) {
            return {
                type: 'BinaryExpression',
                operator: '==',
                left: { type: 'NumberLiteral', value: 0 },
                right: { type: 'NumberLiteral', value: 1 }
            };
        } else {
            var ors = _.reduce(equalities, (or, elm) => {
                return {
                    type: 'BinaryExpression',
                    operator: 'OR',
                    left: or,
                    right: elm
                };
            }, equalities.shift());
            return ors;
        }
    }

    visitFilterLiteral(node) {
        if (this._hasIn(node.ast)) { node.ast = this._rewriteIn(node.ast); }
        this.visit(node.ast);
    }

    visitUnaryExpression(node) {
        if (this._hasIn(node.argument)) { node.argument = this._rewriteIn(node.argument); }
        this.visit(node.argument);
    }

    visitBinaryExpression(node) {
        if (this._hasIn(node.right)) { node.right = this._rewriteIn(node.right); }
        if (this._hasIn(node.left)) { node.left = this._rewriteIn(node.left); }

        this.visit(node.left);
        this.visit(node.right);
    }

    visit(node) {
        this[`visit${node.type}`].apply(this, arguments);
    }
}

module.exports = Rewriter;
