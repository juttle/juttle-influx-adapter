var _ = require('underscore');

/*
 * Compiles juttle options & filter ASTs to influx select queries.
 */
var Compiler = function() {
    this.ops = {
            '==': '=',
            'AND': 'AND',
            'OR': 'OR',
            'NOT': undefined,
            '!=': '!=',
            '<' : '<',
            '>' : '>',
            '>=' : '>=',
            '<=' : '<=',
            '=~' : '=~',
            '!~' : '!~'
    };
}

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
}

Compiler.prototype._walk = function(node) {
    if (!node) { return; }
    switch (node.type) {
        case 'FilterLiteral':
            return this._walk(node.ast);
        case 'ExpressionFilterTerm':
            return this._walk(node.expression);
        case 'BinaryExpression':
            var op = node.operator;
            var str = "";

            var left = this._walk(node.left);
            var right = this._walk(node.right);

            // Convert globs to regexps
            if ((op === '!~' || op === '=~') && node.right.type == 'StringLiteral') {
                right = '/' + node.right.value + '/';
            }

            if (op === 'in' && node.right.type == 'ArrayLiteral' && node.left.type == 'Variable') {
                right = '(' + _.map(right, function(v) { return left + ' = ' + v; }).join(' OR ') + ')';
                left  = ''
            }

            str += (node.left.type == 'BinaryExpression') ? '(' + left  + ')' : left;
            str += ' ' + (this.ops[op] || '') + ' ';
            str += (node.right.type == 'BinaryExpression') ? '(' + right + ')' : right;

            return str;
        case 'Variable':
            return '"' + node.name + '"';
        case 'UnaryExpression':
            return '( ' + ops[node.operator] + ' ' + this._walk(node.expression) + ' )';
        case 'NumericLiteral':
            return node.value;
        case 'ArrayLiteral':
            return node.elements.map(this._walk.bind(this));
        case 'StringLiteral':
            return '"' + node.value + '"';
        case 'RegularExpressionLiteral':
            return '/' + node.value + '/';
        default:
            console.log('Unknown node:' + node.type);
            return node.value;
    };
};

Compiler.prototype._buildSelect = function(options, filter_opts) {
    var fields = options.fields || '*';

    if (_.isArray(fields)) { fields = fields.join(',') }

    return ['SELECT', fields].join(' ');
};

Compiler.prototype._buildWhere = function(options, filter_opts) {
    var root = filter_opts.filter_ast;

    if (root) {
        return ['WHERE', this._walk(root)].join(' ');
    } else {
        return null;
    }
}

Compiler.prototype._buildFrom = function(options, filter_ast) {
    var measurements = options.measurements || '/.*/';

    if (_.isArray(measurements)) { measurements = measurements.join(','); }

    return ['FROM', measurements].join(' ');
}

Compiler.prototype._buildLimit = function(options, filter_ast) {
    if (options.limit) {
        return ['LIMIT', options.limit].join(' ');
    } else {
        return null;
    }
}

Compiler.prototype._buildOffset = function(options, filter_ast) {
    if (options.offset) {
        return ['OFFSET', options.offset].join(' ');
    } else {
        return null;
    }
}

Compiler.prototype._buildOrderBy = function(options, filter_ast) {
    return null;
};

/*
Compiler.prototype._isMeasurement = function(str) {
    return this._isMeasurementName(str) || this._is
};

Compiler.prototype._isIdentifier = function(str) {
    return this._isUnquotedIdentifier || this._isQuotedIdentifier;
};

Compiler.prototype._isUnquotedIdentifier = function(str) {
    return true;
};

Compiler.prototype._isQuotedIdentifier = function(str) {
    return true;
};
*/

module.exports = Compiler;
