"use strict";

var LmdbQueue = require('../'),
    Consumer = LmdbQueue.Consumer;

var consumer = new Consumer({ path: __dirname + '/test-data', topic: 'test', name: 'test2', dataType: LmdbQueue.BUFFER_TYPE, chunkSize: 64 * 1024 * 1024, batchSize: 16 });

setInterval(function() {
  var message=[];
  var buf = consumer.pop();
  var dup = new Buffer(buf.length);
  buf.copy(dup);
  message.push(buf);
  message.push(dup);
},100);
