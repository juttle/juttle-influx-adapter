/*
 * Given a juttle filter AST, rewrites the following statements to equivalent
 * forms supported by influxdb.  This makes the acutal building of query
 * strings easier.
 *
 *    * Distributes negation into the statements.
 *      I.e. fixing 'select value from cpu where not "key" = 1'
 *      not working in 0.9.0.
 *    * Replaces 'in' operator with equivalent series of 'OR's
 *      In is unsupported by Influx.
 */

var InRewriter = require('./rewrite/in');
var NotRewriter = require('./rewrite/not');

var Rewriter = function(ast) {
    this.notRewriter = new NotRewriter();
    this.inRewriter  = new InRewriter();
};

Rewriter.prototype.rewrite = function(_ast) {
    var ast = JSON.parse(JSON.stringify(_ast));

    // Order matters here. The second rewrite assumes there is no 'in'
    // operator left in the tree, as there is no operator corresponding
    // to 'not in'.

    this.inRewriter.visit(ast);
    this.notRewriter.visit(ast);

    return ast;
};

module.exports = Rewriter;
