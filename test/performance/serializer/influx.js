'use strict';

var Benchmark = require('benchmark');
var withAdapterAPI = require('juttle/test').utils.withAdapterAPI;

withAdapterAPI(() => {
    global.Serializer = require('../../../lib/serializer');
    global.JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;

    new Benchmark.Suite().add('serializer.toInflux', {
        setup: () => {
            var serializer = new Serializer();
            var point = { tag: 'one', another: 'two', num: 1.1, _name: 'n', time: new JuttleMoment(Date.now()) };
        },
        fn: () => {
            serializer.toInflux(point, '_name');
        },
        onError: (e) => {
             console.log(e.message)
        }
    }).on('cycle', (event) => {
        console.log(String(event.target));
    }).run();
});
