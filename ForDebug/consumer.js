"use strict";

var LmdbQueue = require('../'),
    Consumer = LmdbQueue.Consumer;

var consumer = new Consumer({ path: __dirname + '/test-data', topic: 'test', name: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 64 * 1024 * 1024, batchSize: 1024 * 16 }),
    start = Date.now();

while(1) {
  var buf = consumer.pop();
  console.log(buf.length);
}
