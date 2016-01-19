
var Read = require('./lib/read');
var Write = require('./lib/write');
var config = require('./lib/config');

function InfluxAdapter(cfg) {
    config.set(cfg);
    return {
        name: 'influx',
        read: Read,
        write: Write
    };
}

module.exports = InfluxAdapter;
