'use strict';

//
// Simple module to contain the configuration for the adapter.
//
var _ = require('underscore');

var config = {};

module.exports = {
    set(cfg) {
        _.extend(config, cfg);
    },
    get() {
        return config;
    }
};
