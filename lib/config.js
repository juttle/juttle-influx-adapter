//
// Simple module to contain the configuration for the adapter.
//
var _ = require('underscore');
var config = {};

function set(cfg) {
    _.extend(config, cfg);
}

function get() {
    return config;
}

module.exports = {
    set: set,
    get: get
};
