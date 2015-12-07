var _ = require('underscore');
var Rewriter = require('../ast/rewrite');
var WhereBuilder = require('./where');
var Linter = require('../ast/lint');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;

/*
 * Compiles juttle options & filter ASTs to influx select queries.
 */
var SelectBuilder = function() {
    this.rewriter = new Rewriter();
    this.whereBuilder  = new WhereBuilder();
    this.linter = new Linter();
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

// This is pretty gross
var ms = new JuttleMoment({isDuration: true, raw: 1, raw2: 'ms'});

SelectBuilder.prototype._where = function(options, filter_opts) {
    var filters = [];

    var from = filter_opts.from;
    if (from) {
        // This would be cleaner if influx supported >= on time
        filters.push('time > \'' + from.subtract(ms).moment.format('YYYY-MM-DD HH:mm:ss.SSS') + '\'');
    }

    var to = filter_opts.to;
    if (to) {
        filters.push('time < \'' + to.moment.format('YYYY-MM-DD HH:mm:ss.SSS') + '\'');
    }

    var root = filter_opts.filter_ast;
    if (root) {
        this.linter.lint(root);
        filters.push(this.whereBuilder.build(this.rewriter.rewrite(root)));
    }

    if (filters.length === 0) {
        return null;
    } else {
        return 'WHERE ' + filters.join(' AND ');
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
