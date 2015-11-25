var juttle_test_utils = require('juttle/test/runtime/specs/juttle-test-utils');
var check_juttle = juttle_test_utils.check_juttle;
var expect = require('chai').expect;
var influxdb = require('../index.js');
var retry = require('bluebird-retry');

var Juttle = require('juttle/lib/runtime').Juttle;

Juttle.backends.register('influxdb', influxdb({
    url: 'http://localhost:8086/',
}, Juttle));

describe('influxdb-backend tests', function () {
    skip('reports nonexistent database', function() {});

    it('reads no points initially', function() {
        return check_juttle({
            program: 'readx influxdb -db "testdb" -raw "SELECT * FROM /.*/"'
        })
        .then(function(result) {
            expect(result.errors.length).equal(0);
            expect(result.sinks.table.length).equal(0);
        });
    });
});
