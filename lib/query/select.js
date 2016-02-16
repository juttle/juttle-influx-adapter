'use strict';

var _ = require('underscore');
var Rewriter = require('../ast/rewrite');
var WhereBuilder = require('./where');
var FromBuilder = require('./from');
var Linter = require('../ast/lint');
var TimeFilter = require('./time-filter');

/*
 * Compiles juttle options & filter ASTs to influx select queries.
 */
class SelectBuilder {
    constructor(options) {
        this.rewriter = new Rewriter();
        this.whereBuilder = new WhereBuilder(options.adapterName);
        this.fromBuilder = new FromBuilder();
        this.linter = new Linter();
        this.timeFilter = new TimeFilter();
    }

    /*
     * select_stmt = "SELECT" fields from_clause [ into_clause *] [ where_clause ]
     *               [ group_by_clause *] [ order_by_clause ] [ limit_clause ]
     *               [ offset_clause *] [ slimit_clause *] [ soffset_clause *]
     * Constructs marked with * are not supported.
     */
    build(options, filter_opts) {
        options = options || {};
        filter_opts = filter_opts || {};

        // Raw query, nothing to do
        if (options.raw) { return options.raw; }

        if (!options.nameField) { throw new Error('Missing required option: nameField'); }

        var root  = filter_opts.filter_ast;

        if (root) {
            this.linter.lint(root, _.pick(options, 'nameField'));
        }

        return _.compact([
            this._select(options, filter_opts),
            this._from(options, filter_opts),
            this._where(options, filter_opts),
            this._orderBy(options, filter_opts),
            this._limit(options, filter_opts),
            this._offset(options, filter_opts)
        ]).join(' ');
    }

    _select(options, filter_opts) {
        var fields = options.fields || '*';

        if (_.isArray(fields)) { fields = fields.join(','); }

        return ['SELECT', fields].join(' ');
    }

    _where(options, filter_opts) {
        var root  = filter_opts.filter_ast;
        var where = '';

        if (root) {
            // Rewrite juttle expressions not understood by influx
            root = this.rewriter.rewrite(root);

            // Get query filter
            where = this.whereBuilder.build(root, _.pick(options, 'nameField'));
        }

        // FIXME: It'd be nicer (but more complex) to handle this on an AST level
        where = this.timeFilter.addFilter(where, options.from, options.to);

        if (where === '') {
            return null;
        } else {
            return ['WHERE', where].join(' ');
        }
    }

    _from(options, filter_opts) {
        var root = filter_opts.filter_ast;
        var from = "";

        if (root) {
            from = this.fromBuilder.build(root, _.pick(options, 'nameField'));
        }

        return ['FROM', from || '/.*/'].join(' ');
    }

    _limit(options, filter_ast) {
        if (options.limit) {
            return ['LIMIT', options.limit].join(' ');
        } else {
            return null;
        }
    }

    _offset(options, filter_ast) {
        if (options.offset) {
            return ['OFFSET', options.offset].join(' ');
        } else {
            return null;
        }
    }

    _orderBy(options, filter_ast) {
        if (options.orderBy) {
            // FIXME: handle arrays
            var orderDirection = options.orderDescending ? 'DESC' : 'ASC';
            return ['ORDER BY', options.orderBy, orderDirection].join(' ');
        } else {
            return null;
        }
    }
}

module.exports = SelectBuilder;
