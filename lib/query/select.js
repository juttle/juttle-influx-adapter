var _ = require('underscore');
var Rewriter = require('../ast/rewrite');
var WhereBuilder = require('./where');

/*
 * Compiles juttle options & filter ASTs to influx select queries.
 */
var SelectBuilder = function() {
    this.rewriter = new Rewriter();
    this.whereBuilder  = new WhereBuilder();
};

/*
 * select_stmt = "SELECT" fields from_clause [ into_clause *] [ where_clause ]
 *               [ group_by_clause *] [ order_by_clause ] [ limit_clause ]
 *               [ offset_clause *] [ slimit_clause *] [ soffset_clause *]
 * Constructs marked with * are not supported.
 */
SelectBuilder.prototype.build = function(options, filter_opts) {
    options = options || {};
    filter_opts = filter_opts || {};

    return _.compact([
        this._select(options, filter_opts),
        this._from(options, filter_opts),
        this._where(options, filter_opts),
        this._orderBy(options, filter_opts),
        this._limit(options, filter_opts),
        this._offset(options, filter_opts)
    ]).join(' ');
};

SelectBuilder.prototype._select = function(options, filter_opts) {
    var fields = options.fields || '*';

    if (_.isArray(fields)) { fields = fields.join(','); }

    return ['SELECT', fields].join(' ');
};

SelectBuilder.prototype._where = function(options, filter_opts) {
    var root = filter_opts.filter_ast;

    if (root) {
        return ['WHERE', this.whereBuilder.build(this.rewriter.rewrite(root))].join(' ');
    } else {
        return null;
    }
};

SelectBuilder.prototype._from = function(options, filter_ast) {
    var measurements = options.measurements || '/.*/';

    if (_.isArray(measurements)) { measurements = measurements.join(','); }

    return ['FROM', measurements].join(' ');
};

SelectBuilder.prototype._limit = function(options, filter_ast) {
    if (options.limit) {
        return ['LIMIT', options.limit].join(' ');
    } else {
        return null;
    }
};

SelectBuilder.prototype._offset = function(options, filter_ast) {
    if (options.offset) {
        return ['OFFSET', options.offset].join(' ');
    } else {
        return null;
    }
};

SelectBuilder.prototype._orderBy = function(options, filter_ast) {
    // FIXME
};

module.exports = SelectBuilder;
