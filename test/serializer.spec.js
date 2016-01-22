'use strict';

var expect = require('chai').expect;
var Serializer = require('../lib/serializer');
var JuttleMoment = require('juttle/lib/moment').JuttleMoment;

describe('serialization', function() {
    describe('to juttle', function() {
        var serializer = new Serializer();

        it('requires all args', function() {
            expect(serializer.toJuttle.bind(serializer, null, [], [])).to.throw(Error, /name argument required/);
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
            expect(point).to.deep.equal({ key1: 'val1', key2: 'val2' });
        });

        describe('nameField option', function() {
            it('stores name to specified field', function() {
                var serializer = new Serializer({ nameField: '_name' });
                var point = serializer.toJuttle('test', ['key'], ['val']);
                expect(point._name).to.equal('test');
            });
        });
    });

    describe('to influx', function() {
        var serializer = new Serializer({ nameField: '_name' });

        it('requires name', function() {
            var point = { num: 1 };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /point is missing a name/);
        });

        it('name is not in the values', function() {
            var point = { num: 1, _name: 'test' };
            expect(serializer.toInflux(point).split(" ")[1]).to.not.include('test');
        });

        it('requires at least one field', function() {
            var point = { _name: 'n' };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /point requires at least one field/);
        });

        it('requires at least one field w/ timestamp', function() {
            var point = { _name: 'n', time: new JuttleMoment(Date.now()) };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /point requires at least one field/);
        });

        it('escapes commas in names', function() {
            var point = { num: 1.1, _name: 'n,' };
            expect(serializer.toInflux(point)).to.equal('n\\, num=1.1');
        });

        it('escapes commas in tag keys and values', function() {
            var point = { 'str,': 'val,', num: 1.1, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n,str\\,=val\\, num=1.1');
        });

        it('escapes spaces in names', function() {
            var point = { num: 1.1, _name: 'n ' };
            expect(serializer.toInflux(point)).to.equal('n\\  num=1.1');
        });

        it('escapes spaces in tag keys and values', function() {
            var point = { num: 1.1, ' str ': ' val ', _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n,\\ str\\ =\\ val\\  num=1.1');
        });

        it('treats numeric values as fields', function() {
            var point = { num: 1.1, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n num=1.1');
        });

        it('treats string values as tags', function() {
            var point = { str: 'val', num: 1.1, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n,str=val num=1.1');
        });

        it('handles multiple values', function() {
            var point = { num: 1.1, another: 2.2, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n num=1.1,another=2.2');
        });

        it('handles multiple tags', function() {
            var point = { tag: 'one', another: 'two', num: 1.1, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n,tag=one,another=two num=1.1');
        });

        it('serializes timestamp with milisecond precision', function() {
            var now = new JuttleMoment(Date.now() / 1000);
            var point = { num: 1.1, time: now, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n num=1.1 ' + now.unixms());
        });

        it('serializes time as unix timestamps with milisecond precision', function() {
            var t = Date.now();

            var start = new JuttleMoment(t / 1000);
            var end   = new JuttleMoment(t / 1000 + 10);

            var point = { start: start, end: end, _name: 'n' };

            var vals   = serializer.toInflux(point).split(' ')[1];
            var endstr = vals.split(',')[1];
            var time   = endstr.split('=')[1];

            expect(time).to.equal(end.unixms() + '');
        });

        it('serializes boolean values', function() {
            var point = { t: true, f: false, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n t=t,f=f');
        });

        it('escapes double quotes in strings', function() {
            var serializer = new Serializer({ valFields: 'str', nameField: '_name' });
            var point = { str: 'can haz "quotes"', _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n str="can haz \\"quotes\\""');
        });

        it.skip('sorts tags Go style', function() {
            // Lexicographical sort on byte representation:
            // https://golang.org/pkg/bytes/#Compare
        });

        it('handles serialization of large floats', function() {
            var point = { num: 1e21, _name: 'n' };
            expect(serializer.toInflux(point)).to.equal('n num=1e+21');
        });

        it('serializing object fields throws an error', function() {
            var point = { obj: { k: "v" }, _name: 'n' };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /not supported/);
        });

        it('serializing array fields throws an error', function() {
            var point = { arr: [1,2,3], _name: 'n' };
            expect(serializer.toInflux.bind(serializer, point)).to.throw(Error, /not supported/);
        });

        describe('nameField option', function() {
            it('defaults to name', function() {
                var serializer = new Serializer();
                var point = { num: 1, name: 'n' };
                expect(serializer.toInflux(point).split(" ")[0]).to.equal('n');
            });
        });

        describe('name option', function() {
            it('is used as a fallback w/o point containing nameField', function() {
                var serializer = new Serializer({ name: 'n' });
                var point = { num: 1 };
                expect(serializer.toInflux(point).split(" ")[0]).to.equal('n');
            });
        });

        describe('intFields option', function() {
            it('serializes numbers as ints', function() {
                var serializer = new Serializer({ intFields: 'num,another_num', nameField: '_name' });
                var point = { num: 1, another_num: 2, fp: 1, _name: 'n' };
                expect(serializer.toInflux(point)).to.equal('n num=1i,another_num=2i,fp=1');
            });
        });

        describe('valFields option', function() {
            it('serializes strings as fields', function() {
                var serializer = new Serializer({ valFields: 'str,another_str', nameField: '_name' });
                var point = { str: 'one', another_str: 'two', tag: 'tag', _name: 'n' };
                expect(serializer.toInflux(point)).to.equal('n,tag=tag str="one",another_str="two"');
            });
        });
    });
});
