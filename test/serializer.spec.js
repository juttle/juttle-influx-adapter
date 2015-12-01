var expect = require('chai').expect;
var Serializer = require('../lib/serializer');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;

describe('serialization', function() {
    describe('to juttle', function() {
        var serializer = new Serializer();

        it('requires all args', function() {
            expect(serializer.toJuttle.bind(serializer, null, [], [])).to.throw(Error, /measurement argument required/);
            expect(serializer.toJuttle.bind(serializer, 'test', null, [])).to.throw(Error, /keys argument required/);
            expect(serializer.toJuttle.bind(serializer, 'test', [], null)).to.throw(Error, /values argument required/);
        });

        it('converts time to a juttle moment', function() {
            var point = serializer.toJuttle('test', ['time'], [Date.now()]);
            expect(point.time).to.not.equal(undefined);
            expect(point.time).to.be.an.instanceof(JuttleMoment);
        });

        it('converts nulls to empty strings', function() {
            var point = serializer.toJuttle('test', ['key'], [null]);
            expect(point.key).to.equal('');
        });

        it('stores key value pairs into object', function() {
            var point = serializer.toJuttle('test', ['key1', 'key2'], ['val1', 'val2']);
            expect(point).to.deep.equal({key1: 'val1', key2: 'val2'});
        });

        describe('measurementField option', function() {
            it('stores measurement name to specified field', function() {
                var serializer = new Serializer({measurementField: '_measurement'});
                var point = serializer.toJuttle('test', ['key'], ['val']);
                expect(point._measurement).to.equal('test');
            });
        });
    });

    describe('to influx', function() {
        var serializer = new Serializer({measurementField: '_measurement'});

        it('requires measurement', function() {
            var point = { num: 1 };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /point is missing a measurement/);
        });

        it('measurement is not in the values', function() {
            var point = { num: 1, _measurement: 'test' };
            expect(serializer.toInflux(point).split(" ")[1]).to.not.include('test');
        });

        it('requires at least one field', function() {
            var point = { _measurement: 'm' };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /point requires at least one field/);
        });

        it('requires at least one field w/ timestamp', function() {
            var point = { _measurement: 'm', time: new JuttleMoment(Date.now()) };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /point requires at least one field/);
        });

        it('escapes commas in measurements', function() {
            var point = { num: 1.1, _measurement: 'm,' };
            expect(serializer.toInflux(point)).to.equal('m\\, num=1.1');
        });

        it('escapes commas in tag keys and values', function() {
            var point = { 'str,': 'val,', num: 1.1, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m,str\\,=val\\, num=1.1');
        });

        it('escapes spaces in measurements', function() {
            var point = { num: 1.1, _measurement: 'm ' };
            expect(serializer.toInflux(point)).to.equal('m\\  num=1.1');
        });

        it('escapes spaces in tag keys and values', function() {
            var point = { num: 1.1, ' str ': ' val ', _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m,\\ str\\ =\\ val\\  num=1.1');
        });

        it('treats numeric values as fields', function() {
            var point = { num: 1.1, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m num=1.1');
        });

        it('treats string values as tags', function() {
            var point = { str: 'val', num: 1.1, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m,str=val num=1.1');
        });

        it('handles multiple values', function() {
            var point = { num: 1.1, another: 2.2, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m num=1.1,another=2.2');
        });

        it('handles multiple tags', function() {
            var point = { tag: 'one', another: 'two', num: 1.1, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m,tag=one,another=two num=1.1');
        });

        it('serializes timestamp with milisecond precision', function() {
            var now = new JuttleMoment(Date.now());
            var point = { num: 1.1, time: now, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m num=1.1 ' + now.unixms());
        });

        it('serializes time as unix timestamps with millisecond precision', function() {
            var x = Date.now();
            var point = { start: x, end: x + 10, _measurement: 'm' };
        });

        it('serializes boolean values', function() {
            var point = { t: true, f: false, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m t=t,f=f');
        });

        it('escapes double quotes in strings', function() {
            var serializer = new Serializer({valFields: 'str', measurementField: '_measurement'});
            var point = { str: 'can haz "quotes"', _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m str="can haz \\"quotes\\""');
        });

        it.skip('throws on non-flat structures', function() {
        });

        it.skip('sorts tags Go style', function() {
            var point = { str: 'can haz "quotes"', _measurement: 'm' };
        });

        it('handles serialization of large floats', function() {
            var point = { num: 1e21, _measurement: 'm' };
            expect(serializer.toInflux(point)).to.equal('m num=1e+21');
        });

        describe('intFields option', function() {
            it('serializes numbers as ints', function() {
                var serializer = new Serializer({intFields: 'num,another_num', measurementField: '_measurement'});
                var point = { num: 1, another_num: 2, fp: 1, _measurement: 'm' };
                expect(serializer.toInflux(point)).to.equal('m num=1i,another_num=2i,fp=1');
            });
        });

        describe('valFields option', function() {
            it('serializes strings as fields', function() {
                var serializer = new Serializer({valFields: 'str,another_str', measurementField: '_measurement'});
                var point = { str: 'one', another_str: 'two', tag: 'tag', _measurement: 'm' };
                expect(serializer.toInflux(point)).to.equal('m,tag=tag str="one",another_str="two"');
            });
        });

        describe('precision option', function() {
            it('serializes floats to 4 decimal places by default', function() {
                var point = { num: 1.1, _measurement: 'm' };
                expect(serializer.toInflux(point)).to.equal('m num=1.1');
            });
        });
    });
});
