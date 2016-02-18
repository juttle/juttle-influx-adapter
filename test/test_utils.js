var SemanticPass = require('juttle/lib/compiler/semantic');
var FilterSimplifier = require('juttle/lib/compiler/filters/filter-simplifier');

var parser = require('juttle/lib/parser');
var semantic = new SemanticPass({ now: Date.now() });
var simplifier = new FilterSimplifier();

module.exports = {
    parseFilter(code) {
        var root = parser.parseFilter(code);
        root.ast = semantic.sa_expr(root.ast);
        root.ast = simplifier.simplify(root.ast);
        return root;
    }
};
