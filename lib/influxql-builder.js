var _ = require('underscore');
var ASTRewriter = require('./ast-rewriter');
var ASTPrinter  = require('./ast-printer');

/*
 * Compiles juttle options & filter ASTs to influx select queries.
 */
var Compiler = function() {
    this.rewriter = new ASTRewriter();
    this.printer  = new ASTPrinter();
};

/*
 * select_stmt = "SELECT" fields from_clause [ into_clause *] [ where_clause ]
 *               [ group_by_clause *] [ order_by_clause ] [ limit_clause ]
 *               [ offset_clause *] [ slimit_clause *] [ soffset_clause *]
 * Constructs marked with * are not supported.
 */
Compiler.prototype.build = function(options, filter_opts) {
    options = options || {};
    filter_opts = filter_opts || {};

    return _.compact([
        this._buildSelect(options, filter_opts),
        this._buildFrom(options, filter_opts),
        this._buildWhere(options, filter_opts),
        this._buildOrderBy(options, filter_opts),
        this._buildLimit(options, filter_opts),
        this._buildOffset(options, filter_opts),
    ]).join(' ');
};

Compiler.prototype._buildSelect = function(options, filter_opts) {
    var fields = options.fields || '*';

    if (_.isArray(fields)) { fields = fields.join(','); }

    return ['SELECT', fields].join(' ');
};

Compiler.prototype._buildWhere = function(options, filter_opts) {
    var root = filter_opts.filter_ast;

    if (root) {
        return ['WHERE', this.printer.visit(this.rewriter.visit(root))].join(' ');
    } else {
        return null;
    }
};

Compiler.prototype._buildFrom = function(options, filter_ast) {
    var measurements = options.measurements || '/.*/';

    if (_.isArray(measurements)) { measurements = measurements.join(','); }

    return ['FROM', measurements].join(' ');
};

Compiler.prototype._buildLimit = function(options, filter_ast) {
    if (options.limit) {
        return ['LIMIT', options.limit].join(' ');
    } else {
        return null;
    }
};

Compiler.prototype._buildOffset = function(options, filter_ast) {
    if (options.offset) {
        return ['OFFSET', options.offset].join(' ');
    } else {
        return null;
    }
};

Compiler.prototype._buildOrderBy = function(options, filter_ast) {
    // FIXME
};

module.exports = Compiler;
