"use strict";

var nativeModule = require('../build/Release/lmdb-queue.node'),
    NativeConsumer = nativeModule.Consumer;

function Consumer(opt) {
  this._nativeConsumer = new NativeConsumer(opt);
  this._popFnName = opt.dataType === nativeModule.BUFFER_TYPE ? 'popBuffer' : 'popString';
}

Consumer.prototype = {
  offset: function () {
    return this._nativeConsumer.offset();
  },

  pop: function () {
    return this._nativeConsumer[this._popFnName]();
  },

  updateOffset: function () {
    return this._nativeConsumer.updateOffset();
  }
};

module.exports = Consumer;
