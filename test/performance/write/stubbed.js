'use strict';

var Benchmark = require('benchmark');
var withAdapterAPI = require('juttle/test').utils.withAdapterAPI;
var Promise = require('bluebird');

withAdapterAPI(() => {
    global._ = require('underscore');
    global.JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
    global.AdapterWrite = require('../../../lib/write');
    global.Config = require('../../../lib/config');

    global.pointsInBatch = 1000;
    global.iterations = 0;

    Config.set({url: 'http://localhost:8086/'});

    new Benchmark.Suite().add('AdapterWrite.write, stubbed request, 1k points/call', {
        setup: () => {
            var adapterWrite = new AdapterWrite({}, {});

            adapterWrite.request = (reqUrl, chunk) => { chunk.join('\n'); return Promise.resolve(); };

            var points = _.times(pointsInBatch, (i) => {
                return {
                    tag: 'one' + (iterations * pointsInBatch) + i,
                    another: 'two' + (iterations * pointsInBatch) + i,
                    num: 1.1 + (iterations * pointsInBatch) + i,
                    name: 'n',
                    time: new JuttleMoment(iterations * pointsInBatch + i)
                }
            });
        },
        teardown: () => {
            iterations += 1;
        },
        fn: (deferred) => {
            adapterWrite.write(points).then(function() { deferred.resolve(); });
        },
        onError: (e) => {
            console.log(e.message)
        },
        defer: true
    }).on('cycle', (event) => {
        console.log(String(event.target));
    }).run();
});
