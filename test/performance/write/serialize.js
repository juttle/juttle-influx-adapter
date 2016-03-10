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

    new Benchmark.Suite().add('AdapterWrite.serialize, 1k points/call', {
        setup: () => {
            var adapterWrite = new AdapterWrite({}, {});
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
        fn: () => {
            adapterWrite.serialize(points);
        },
        onError: (e) => {
            console.log(e.message)
        }
    }).on('cycle', (event) => {
        console.log(String(event.target));
    }).run();
});
