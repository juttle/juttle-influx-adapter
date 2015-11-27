var _ = require('underscore');

var Generator = {
    chr: function() { return String.fromCharCode(32 + Math.floor(Math.random() * 95)) },
    str: function(n) { n = n || 32; var s = ''; for (var i = 0; i < Math.floor(Math.random() * n); i++) { s += g.chr(); }; return s; },
    num: function(min, max) { min = min || -1; max = max || 1; return Math.random() * (max - min) + min; },
    time: function() { return new Date(Math.floor(Math.random() * Math.pow(2, 32) * 1000)); },
    type: function() { return _.sample(['str', 'num', 'time']); },
    point: function() {
        var n = Math.floor(g.num(2, 12));
        var has_t = g.num() >= 0;
        var p = {};
        for (var i = 0; i < n; i++) {
            var k = g.str();
            var v = g[g.type()]();
            p[k] = v;
        }
        if (has_t) { p.time = g.time(); };
        return p;
    },
};

module.exports = Generator;
