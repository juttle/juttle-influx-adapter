var ASTVisitor = require('juttle/lib/compiler/ast-visitor');
var _ = require('underscore');

var Lints = {
    lint: function(ast) {
        this.visit(ast);
    },

    // No time on LHS.
    visitVariable: function(node) {
        if (node.name === "time") {
            throw new Error('Time field is not allowed in filter expressions, use -from and/or -to');
        }
    }
};

// Duration comparisons are not supported (they could be converted to moments, though)
_.each([
    'MomentLiteral',
    'DurationLiteral'
], function(node) {
    Lints['visit' + node] = function() {
        throw new Error('Filtering by time is not supported');
    };
});

module.exports = ASTVisitor.extend(Lints);
