'use strict';

/*
 * Serializer to and from Juttle/Influx points
 */

var JuttleMoment = require('juttle/lib/moment').JuttleMoment;
var values = require('juttle/lib/runtime/values');
var _ = require('underscore');

var Serializer = function(options) {
    this.options = options || {};

    this.intFields = this.options.intFields ? this.options.intFields.split(',') : [];
    this.valFields = this.options.valFields ? this.options.valFields.split(',') : [];

    this.name = this.options.name;
};

Serializer.prototype.toJuttle = function(name, keys, values, nameField) {
    if (!name) { throw new Error('name argument required'); }
    if (!keys) { throw new Error('keys argument required'); }
    if (!values) { throw new Error('values argument required'); }
    if (!nameField) { throw new Error('nameField argument required'); }

    // FIXME: our sinks can't handle nulls?
    var obj  = _.object(keys, _.map(values, function(v) { return (v === null ? '' : v); }));

    obj[nameField] = name;

    if (obj.time) {
        obj.time = JuttleMoment.parse(obj.time);
    }

    return obj;
};

Serializer.prototype.toInflux = function(point, nameField) {
    if (!nameField) { throw new Error('nameField argument required'); }

    var name = point[nameField] || this.name;
    var timestamp_ns = point.time ? point.time.unixms() : null;

    point = _.omit(point, nameField, 'time');

    var tags = _.pick(point, this._isTag.bind(this));
    var vals = _.omit(point, this._isTag.bind(this));

    if (_.keys(vals).length === 0) { throw new Error('point requires at least one field'); }
    if (!name) { throw new Error('point is missing a name'); }

    var key = _.flatten([name, _.map(tags, this._concatKeyVal)]).map(this._escapeTag);

    var val = _.chain(vals)
               .map(this._serializeValue.bind(this))
               .object()
               .map(this._concatKeyVal)
               .value();

    return _.compact([key.join(","), val.join(","), timestamp_ns]).join(" ");
};

Serializer.prototype._concatKeyVal = function(v, k) {
    return k + '=' + v;
};

Serializer.prototype._serializeValue = function(v, k) {
    if (values.isBoolean(v)) {
        return [k, (v ? 't' : 'f')];
    } else
    if (values.isString(v)) {
        return [k, '"' + this._escapeString(v) + '"'];
    } else
    if (this._isInt(v, k)) {
        return [k, Math.floor(v) + 'i'];
    } else
    if (values.isDate(v) || values.isDuration(v)) {
        return [k, v.unixms()];
    } else
    if (values.isObject(v) || values.isArray(v)) {
        throw new Error('Serializing array and object fields is not supported.');
    } else {
        return [k, v];
    }
};

// https://influxdb.com/docs/v0.9/write_protocols/line.html#key
Serializer.prototype._escapeTag = function(str) {
    var specialChars = /[\s,]/g;
    return str.replace(specialChars, function(m) { return "\\" + m; });
};

// https://influxdb.com/docs/v0.9/write_protocols/line.html#fields (Strings)
Serializer.prototype._escapeString = function(str) {
    return str.replace(/"/g, '\\"');
};

Serializer.prototype._isTag = function(val, key, obj) {
    return _.isString(val) && !_.contains(this.valFields, key);
};

Serializer.prototype._isInt = function(val, key, obj) {
    return _.isNumber(val) && _.contains(this.intFields, key);
};

module.exports = Serializer;
