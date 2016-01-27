'use strict';

var optimizer = {
    optimize_head(read, head, graph, info) {
        var limit = graph.node_get_option(head, 'arg');

        var opt = { type: 'head', options: { limit: limit, orderBy: 'time', orderDescending: false } };

        Object.assign(info, opt);

        return true;
    },

    // The tail optimization relies on an explicit sort by time that occurs after
    // fetching points, so we don't have to reverse the result set.
    optimize_tail(read, tail, graph, info) {
        var limit = graph.node_get_option(tail, 'arg');

        var opt = { type: 'tail', options: { limit: limit, orderBy: 'time', orderDescending: true} };

        Object.assign(info, opt);

        return true;
    }
};

module.exports = optimizer;
