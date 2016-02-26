'use strict';

var Benchmark = require('benchmark');
var withAdapterAPI = require('juttle/test').utils.withAdapterAPI;

withAdapterAPI(() => {
    global.Serializer = require('../../../lib/serializer');

    new Benchmark.Suite().add('serializer.toJuttle', {
        setup: () => {
            var serializer = new Serializer();
            var name = "cpu";
            var keys = ["tag", "another", "num", "time"];
            var values = ["one", "two", 1.1, Date.now()];
            var nameField = "name";
        },
        fn: () => {
            serializer.toJuttle(name, keys, values, nameField);
        },
        onError: (e) => {
            console.log(e.message)
        }
    }).on('cycle', (event) => {
        console.log(String(event.target));
    }).run();
});
