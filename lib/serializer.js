'use strict';

/*
 * Serializer to and from Juttle/Influx points
 */

/* global JuttleAdapterAPI */
var JuttleMoment = JuttleAdapterAPI.types.JuttleMoment;
var values = JuttleAdapterAPI.runtime.values;
var _ = require('underscore');

class Serializer {
    constructor(options) {
        this.options = options || {};

        this.intFields = this.options.intFields ? this.options.intFields.split(',') : [];
        this.valFields = this.options.valFields ? this.options.valFields.split(',') : [];
    }

    toJuttle(name, keys, values, nameField) {
        if (!name) { throw new Error('name argument required'); }
        if (!keys) { throw new Error('keys argument required'); }
        if (!values) { throw new Error('values argument required'); }
        if (!nameField) { throw new Error('nameField argument required'); }

        // FIXME: our sinks can't handle nulls?
        var obj  = _.object(keys, _.map(values, (v) => { return (v === null ? '' : v); }));

        obj[nameField] = name;

        if (obj.time) {
            obj.time = JuttleMoment.parse(obj.time);
        }

        return obj;
    }

    toInflux(point, nameField) {
        if (!nameField) { throw new Error('nameField argument required'); }

        // Measurement name
        var name = point[nameField];
        if (!name) { throw new Error('point is missing a name'); }

        // Timestamp
        var timestamp = point.time ? point.time.unixms() : '';
        var point = _.omit(point, nameField, 'time');

        // Tags, values
        var tags = _.pick(point, (val, key) => {
            return _.isString(val) && !_.contains(this.valFields, key);
        });

        var vals = _.omit(point, (val, key) => {
            return _.isString(val) && !_.contains(this.valFields, key);
        });

        // Drop point early, influx wont accept this
        if (_.keys(vals).length === 0) { throw new Error('point requires at least one field'); }

        // left column - name + tags
        var taggedName = [
            this._escapeTag(name)
        ].concat(
            _.map(tags, (val, key) => {
                return this._escapeTag(`${key}=${val}`);
            })
        ).join(",");

        // middle column
        var values = _.map(vals, (val, key) => {
            return `${key}=${this._serializeValue(key, val, this.intFields)}`;
        }).join(",");

        return `${taggedName} ${values} ${timestamp}`.trim();
    }

    _serializeValue(k, v, intFields) {
        if (values.isBoolean(v)) {
            return (v ? 't' : 'f');
        } else
        if (values.isString(v)) {
            return `"${this._escapeString(v)}"`;
        } else
        if (_.isNumber(v) && _.contains(intFields, k)) {
            return `${Math.floor(v)}i`;
        } else
        if (values.isDate(v) || values.isDuration(v)) {
            return v.unixms();
        } else
        if (values.isRegExp(v)) {
            throw new Error('Serializing regular expression values is not supported.');
        } else
        if (values.isNull(v)) {
            throw new Error('Serializing null values is not supported.');
        } else
        if (values.isObject(v) || values.isArray(v)) {
            throw new Error('Serializing array and object fields is not supported.');
        } else {
            return v;
        }
    }

    // https://influxdb.com/docs/v0.9/write_protocols/line.html#key
    _escapeTag(str) {
        return str.replace(/[\s,]/g, "\\$&");
    }

    // https://influxdb.com/docs/v0.9/write_protocols/line.html#fields (Strings)
    _escapeString(str) {
        return str.replace(/"/g, '\\"');
    }
}

module.exports = Serializer;
