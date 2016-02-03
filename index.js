'use strict';

var Read = require('./lib/read');
var Write = require('./lib/write');
var Optimizer = require('./lib/query/optimize');
var config = require('./lib/config');

function InfluxAdapter(cfg) {
    config.set(cfg);
    return {
        name: 'influx',
        read: Read,
        write: Write,
        optimizer: Optimizer
    };
}

module.exports = InfluxAdapter;
