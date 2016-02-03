var SemanticPass = require('juttle/lib/compiler/semantic');

var parser = require('juttle/lib/parser');
var semantic = new SemanticPass({ now: Date.now() });

module.exports = {
    parseFilter(code) {
        var root = parser.parseFilter(code);
        root.ast = semantic.sa_expr(root.ast);
        return root;
    }
};
