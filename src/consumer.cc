#include <string.h>

#include "topic.h"
#include "consumer.h"

using namespace std;

Consumer::Consumer(const std::string& root, const std::string& topic,
                   const std::string& name, size_t id, size_t batchSize,
                   TopicOpt* opt)
    : _topic(EnvManager::getEnv(root)->getTopic(topic)),
      _name(name),
      _id(id),
      _batchSize(batchSize),
      _current(0),
      _lastOffset(0),
      _env(nullptr),
      _db(0),
      _rtxn(nullptr),
      _cursor(nullptr) {
  if (opt) {
    _opt = *opt;
  } else {
    /* Default opt */
    _opt.chunkSize = 1024 * 1024 * 1024;
    _opt.chunksToKeep = 8;
  }

  Txn txn(_topic->getEnv(), NULL);
  openHead(&txn);
  if (_lastOffset == 0) _lastOffset = _topic->getConsumerLastOffset(txn, _name, _id, _batchSize);
  std::cout << "in ctor id: " << _id << std::endl;
  std::cout << "in ctor _lastOffset: " << _lastOffset << std::endl;
  if (_lastOffset == 0) {
    _lastOffset = _topic->getConsumerLastOffset(txn, _name, _id, _batchSize);
  }

  _nextOffset = _lastOffset + 1;

  txn.commit();
      }

Consumer::~Consumer() { closeCurrent(); }

void Consumer::pop(BatchType& result, size_t cnt) {
  result.reserve(cnt);
  bool shouldRotate = false;

  {
    Txn txn(_topic->getEnv(), NULL);

    uint64_t phead = _topic->getProducerHead(txn);

    if ((_lastOffset >= phead + 1) || phead == 0) return;

    int rc = _cursor->gte(_lastOffset);

    if (rc == 0) {
      uint64_t offset = 0;

      for (; rc == 0 && cnt > 0; --cnt) {
        offset = _cursor->key<uint64_t>();
        std::cout << "id:" << _id << "offset: " << offset << " _lastOffset: " << _lastOffset << std::endl;
        if ((offset % _batchSize == 0) && (offset != _lastOffset)) {
          offset += 4 * _batchSize;
          break;
        }
        const char* data = (const char*)_cursor->val().mv_data;
        size_t len = _cursor->val().mv_size;
        result.push_back(ItemType(offset, data, len));
        rc = _cursor->next();
      }

      if (offset > 0) {
        _lastOffset = offset + 1;
        std::cout << "id: " << _id << std::endl;
        std::cout << "_lastOffset: " << _lastOffset << std::endl;
        _lastOffset = offset;
        _nextOffset = _lastOffset + 1;
      }
    } else {
      if (rc != MDB_NOTFOUND)
        cout << "Consumer seek error: " << mdb_strerror(rc) << endl;

      if (_lastOffset <= _topic->getProducerHead(txn)) {
        shouldRotate = true;
      }
    }
  }

  if (shouldRotate) {
    rotate();
    pop(result, cnt);
  }
}

void Consumer::updateOffset() {
  Txn txn(_topic->getEnv(), NULL);
  std::cout << "updateOffset" << std::endl;
  _topic->setConsumerHead(txn, _name, _nextOffset);
  txn.commit();
}

void Consumer::openHead(Txn* txn) {
  _current = _topic->getConsumerHeadFileByLastOffset(*txn, _lastOffset, _current);

  char path[4096];
  memset(path, '\0', 4096);
  _topic->getChunkFilePath(path, _current);

  mdb_env_create(&_env);
  mdb_env_set_mapsize(_env, _opt.chunkSize);
  int rc =
      mdb_env_open(_env, path, MDB_RDONLY | MDB_NOSYNC | MDB_NOSUBDIR, 0664);

  int cleared = 0;
  mdb_reader_check(_env, &cleared);

  if (rc != 0) {
    mdb_env_close(_env);
    _env = nullptr;
    printf("Consumer open error.\n%s\n", mdb_strerror(rc));
    return;
  }

  MDB_txn* otxn;
  mdb_txn_begin(_env, NULL, MDB_RDONLY, &otxn);
  mdb_dbi_open(otxn, NULL, MDB_CREATE, &_db);
  mdb_set_compare(otxn, _db, mdbIntCmp<uint64_t>);
  mdb_txn_commit(otxn);

  mdb_txn_begin(_env, NULL, MDB_RDONLY, &_rtxn);
  _cursor = new MDBCursor(_db, _rtxn);
  mdb_txn_reset(_rtxn);
  mdb_txn_renew(_rtxn);
}

void Consumer::closeCurrent() {
  delete _cursor;
  mdb_txn_abort(_rtxn);
  mdb_dbi_close(_env, _db);
  mdb_env_close(_env);
}

void Consumer::rotate() {
  Txn txn(_topic->getEnv(), NULL);
  closeCurrent();
  openHead(&txn);
  txn.commit();
}
