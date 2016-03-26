"use strict";

var LmdbQueue = require('../'),
    Producer = LmdbQueue.Producer;

var producer = new Producer({ backgroundFlush: true, useCache: true, path: __dirname + '/test-data', topic: 'test', dataType: LmdbQueue.STRING_TYPE, chunkSize: 10 * 1024 * 1024 * 1024, chunksToKeep: 8 });

var str = "";

for (var i = 0; i < 100000; i ++ ) {
  str += i;
}

while(1) {
  producer.push(str);
}
