'use strict';

/*
 * Distributes negation in the Juttle AST into subexpressions.
 */

/* global JuttleAdapterAPI */
var ASTVisitor = JuttleAdapterAPI.compiler.ASTVisitor;

class Rewriter extends ASTVisitor {
    negate(op) {
        // Missing in this list is 'in'. It is missing an opposite operator,
        // and also not supported in InfluxDB. We assume it was transformed
        // into ORs sequence in a previous rewrite.
        var negations = {
            '==': '!=',
            '!=': '==',
            '=~': '!~',
            '!~': '=~',
            '<': '>=',
            '>': '<=',
            '<=': '>',
            '>=': '<',
            'AND': 'OR',
            'OR': 'AND'
        };
        return negations[op];
    }

    visitBinaryExpression(node, negate) {
        if (this._hasNegation(node.left)) {
            this.visit(node.left, !negate);
            node.left = this._rewriteNegation(node.left);
        } else {
            this.visit(node.left, negate);
        }

        if (this._hasNegation(node.right)) {
            this.visit(node.right, !negate);
            node.right = this._rewriteNegation(node.right);
        } else {
            this.visit(node.right, negate);
        }

        if (negate) {
            var op = this.negate(node.operator);

            if (!op) { throw new Error(`Operator ${node.operator} negation not supported`); }

            node.operator = op;
        }
    }

    visitUnaryExpression(node, negate) {
        if (this._hasNegation(node.argument)) {
            this.visit(node.argument, !negate);
            node.argument = this._rewriteNegation(node.argument);
        } else {
            this.visit(node.argument, negate);
        }
    }

    _hasNegation(node) {
        return (node.type === 'UnaryExpression' && node.operator === 'NOT');
    }

    _rewriteNegation(node) {
        return node.argument;
    }

    visitFilterLiteral(node, negate) {
        if (this._hasNegation(node.ast)) {
            this.visit(node.ast, !negate);
            node.ast = this._rewriteNegation(node.ast);
        } else {
            this.visit(node.ast, negate);
        }
    }

    visit(node, negate) {
        this[`visit${node.type}`].apply(this, arguments);
    }
}

module.exports = Rewriter;
