'use strict';

/*
 * Adds 'from' and 'to' limits to the query.
 */

var TimeFilter = function() {};

TimeFilter.prototype.addFilter = function(where, from, to) {
    if (where !== '' && (from || to)) {
        where += ' AND ';
    }

    if (from) {
        where += '"time" >= \'' + from.valueOf() + '\'';
    }

    if (from && to) {
        where += ' AND ';
    }

    if (to) {
        where += '"time" < \'' + to.valueOf() + '\'';
    }

    return where;
};

module.exports = TimeFilter;
