var SemanticPass = require('juttle/lib/compiler/semantic');
var FilterSimplifier = require('juttle/lib/compiler/filters/filter-simplifier');

var parser = require('juttle/lib/parser');
var semantic = new SemanticPass({ now: Date.now() });
var simplifier = new FilterSimplifier();

function parseFilter(code) {
    var root = parser.parseFilter(code);
    semantic.sa_expr(root.ast);
    root.ast = simplifier.simplify(root.ast);
    return root;
}

module.exports = parseFilter;
