#include <nan.h>

#include "topic.h"
#include "producer.h"
#include "consumer.h"

using Nan::FunctionCallbackInfo;
using Nan::AsyncQueueWorker;
using Nan::GetCurrentContext;
using Nan::MakeCallback;
using Nan::HandleScope;
using Nan::GetFunction;
using Nan::AsyncWorker;
using Nan::CopyBuffer;
using Nan::Utf8String;
using Nan::Persistent;
using Nan::ObjectWrap;
using Nan::NewBuffer;
using Nan::Callback;
using Nan::Null;
using Nan::Set;
using Nan::New;

using v8::FunctionTemplate;
using v8::Function;
using v8::Integer;
using v8::String;
using v8::Object;
using v8::Number;
using v8::Local;
using v8::Value;

using std::cout;
using std::endl;
using std::map;

using node::Buffer::HasInstance;
using node::Buffer::Data;
using node::Buffer::Length;

enum DataType {
  STRING_TYPE = 1,
  BUFFER_TYPE
};

template<DataType DT> class BatchWrap {
public:
  void push(const Local<Value>& val);

public:
  Producer::BatchType& get() {
    return _batch;
  }

  void reserve(size_t sz) {
    _batch.reserve(sz);
  }

private:
  Producer::BatchType _batch;
};

template<> void BatchWrap<STRING_TYPE>::push(const Local<Value>& val) {
  v8::Local<v8::String> toStr = val->ToString();
  size_t size = toStr->Utf8Length();
  Producer::ItemType item = Producer::ItemType::create(size + 1);
  toStr->WriteUtf8(item.data());
  _batch.push_back(std::move(item));
}

template<> void BatchWrap<BUFFER_TYPE>::push(const Local<Value>& val) {
  _batch.push_back(Producer::ItemType(node::Buffer::Data(val), node::Buffer::Length(val)));
}

class ProducerWrap : public ObjectWrap {
public:
  static void setup(Local<Object> exports) {
    const char* className = "Producer";

    Local<FunctionTemplate> tpl = New<FunctionTemplate>(ctor);
    tpl->SetClassName(New(className).ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    SetPrototypeMethod(tpl, "pushString", push<STRING_TYPE>);
    SetPrototypeMethod(tpl, "pushBuffer", push<BUFFER_TYPE>);

    SetPrototypeMethod(tpl, "pushString2Cache", push2Cache<STRING_TYPE>);
    SetPrototypeMethod(tpl, "pushBuffer2Cache", push2Cache<BUFFER_TYPE>);

    exports->Set(New(className).ToLocalChecked(), tpl->GetFunction());
  }

private:
  static NAN_METHOD(ctor) {
    HandleScope scope;

    Local<Object> opt = info[0]->ToObject();

    Utf8String path(opt->Get(New("path").ToLocalChecked()));
    Utf8String topicName(opt->Get(New("topic").ToLocalChecked()));

    TopicOpt topicOpt{ 1024 * 1024 * 1024, 8 };
    Local<Value> chunkSize = opt->Get(New("chunkSize").ToLocalChecked());
    Local<Value> chunksToKeep = opt->Get(New("chunksToKeep").ToLocalChecked());
    Local<Value> bgFlush = opt->Get(New("backgroundFlush").ToLocalChecked());
    if (chunkSize->IsNumber()) topicOpt.chunkSize = size_t(chunkSize->NumberValue());
    if (chunksToKeep->IsNumber()) topicOpt.chunksToKeep = size_t(chunksToKeep->NumberValue());

    ProducerWrap* ptr = new ProducerWrap(*path, *topicName, &topicOpt, bgFlush->BooleanValue());
    ptr->Wrap(info.This());
    info.GetReturnValue().Set(info.This());
  }

  template<DataType DT> static NAN_METHOD(push) {
    HandleScope scope;

    ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(info.This());

    BatchWrap<DT> batch;
    batch.reserve(info.Length());
    for (int i = 0; i < info.Length(); i++) {
      batch.push(info[i]);
    }

    ptr->_handle.push(batch.get());

    info.GetReturnValue().Set(Null());
  }

  template<DataType DT> static NAN_METHOD(push2Cache) {
    HandleScope scope;

    ProducerWrap* ptr = ObjectWrap::Unwrap<ProducerWrap>(info.This());

    BatchWrap<DT> batch;
    batch.reserve(info.Length());
    for (int i = 0; i < info.Length(); i++) {
      batch.push(info[i]);
    }

    ptr->_handle.push2Cache(batch.get());

    info.GetReturnValue().Set(Null());
  }

private:
  ProducerWrap(const char* path, const char* name, TopicOpt* opt, bool bgFlush) : _handle(path, name, opt) {
    if (bgFlush) _handle.enableBackgroundFlush(std::chrono::milliseconds(200));
  }

  Producer _handle;
};

template<DataType DT> class ReturnMaker {
public:
  Local<Value> static make(const Consumer::ItemType& item);
};

template<> Local<Value> ReturnMaker<STRING_TYPE>::make(const Consumer::ItemType& item) {
  return New(std::get<1>(item)).ToLocalChecked();
}

template<> Local<Value> ReturnMaker<BUFFER_TYPE>::make(const Consumer::ItemType& item) {
  return NewBuffer((char*)std::get<1>(item), std::get<2>(item)).ToLocalChecked();
}

class ConsumerWrap : public ObjectWrap {
public:
  static void setup(Local<Object> exports) {
    const char* className = "Consumer";

    Local<FunctionTemplate> tpl = New<FunctionTemplate>(ctor);
    tpl->SetClassName(New(className).ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    SetPrototypeMethod(tpl, "offset", offset);
    SetPrototypeMethod(tpl, "popString", pop<STRING_TYPE>);
    SetPrototypeMethod(tpl, "popBuffer", pop<BUFFER_TYPE>);

    exports->Set(New(className).ToLocalChecked(), tpl->GetFunction());
  }

private:
  static NAN_METHOD(ctor) {
    HandleScope scope;

    Local<Object> opt = info[0]->ToObject();

    Utf8String path(opt->Get(New("path").ToLocalChecked()));
    Utf8String topicName(opt->Get(New("topic").ToLocalChecked()));
    Utf8String name(opt->Get(New("name").ToLocalChecked()));

    TopicOpt topicOpt{ 1024 * 1024 * 1024, 0 };
    Local<Value> chunkSize = opt->Get(New("chunkSize").ToLocalChecked());
    if (chunkSize->IsNumber()) topicOpt.chunkSize = size_t(chunkSize->NumberValue());

    ConsumerWrap* ptr = new ConsumerWrap(*path, *topicName, *name, &topicOpt);
    Local<Value> batchSize = opt->Get(New("batchSize").ToLocalChecked());
    if (batchSize->IsNumber()) {
      size_t bs = size_t(batchSize->NumberValue());
      if (bs > 0 && bs < 1024 * 1024) ptr->_batchSize = bs;
    }

    ptr->Wrap(info.This());
    info.GetReturnValue().Set(info.This());
  }

  static NAN_METHOD(offset) {
    HandleScope scope;

    ConsumerWrap* ptr = ObjectWrap::Unwrap<ConsumerWrap>(info.This());

    if (ptr->_cur <= ptr->_batch.size()) {
      info.GetReturnValue().Set((New(double(std::get<0>(ptr->_batch.at(ptr->_cur - 1))))));
    }

    info.GetReturnValue().Set(Null());
  }

  template<DataType DT> static NAN_METHOD(pop) {
    HandleScope scope;

    ConsumerWrap* ptr = ObjectWrap::Unwrap<ConsumerWrap>(info.This());

    if (ptr->_cur < ptr->_batch.size()) {
      info.GetReturnValue().Set(ReturnMaker<DT>::make(ptr->_batch.at(ptr->_cur++)));
    }

    ptr->_batch.clear();
    ptr->_cur = 1;
    ptr->_handle.pop(ptr->_batch, ptr->_batchSize);
    if (ptr->_batch.size() > 0) {
      info.GetReturnValue().Set(ReturnMaker<DT>::make(ptr->_batch.at(0)));
    }

    info.GetReturnValue().Set(Null());
  }

private:
  ConsumerWrap(const char* path, const char* topicName, const char* name, TopicOpt* opt) : _handle(path, topicName, name, opt), _cur(0), _batchSize(128) { }

  Consumer _handle;
  Consumer::BatchType _batch;
  size_t _cur, _batchSize;
};

class TopicWrap : public ObjectWrap {
public:
  static void setup(Local<Object> exports) {
    const char* className = "Topic";

    Local<FunctionTemplate> tpl = New<FunctionTemplate>(TopicWrap::ctor);
    tpl->SetClassName(New(className).ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    SetPrototypeMethod(tpl, "status", status);

    exports->Set(New(className).ToLocalChecked(), tpl->GetFunction());
  }

private:
  static NAN_METHOD(ctor) {
    HandleScope scope;

    Local<Object> opt = info[0]->ToObject();

    Utf8String path(opt->Get(New("path").ToLocalChecked()));
    Utf8String topicName(opt->Get(New("topic").ToLocalChecked()));

    TopicWrap *ptr = new TopicWrap(*path, *topicName);

    ptr->Wrap(info.This());
    info.GetReturnValue().Set(Null());
  }

  static NAN_METHOD(status) {
    HandleScope scope;

    TopicWrap* ptr = ObjectWrap::Unwrap<TopicWrap>(info.This());
    TopicStatus st = ptr->_handle->status();

    Local<Object> ret = New<Object>();
    ret->Set(New("producerHead").ToLocalChecked(), New<Number>(double(st.producerHead)));

    Local<Object> consumerHeads = New<Object>();
    for (auto it : st.consumerHeads) {
      consumerHeads->Set(New(it.first).ToLocalChecked(), New<Number>(double(it.second)));
    }
    ret->Set(New("consumerHeads").ToLocalChecked(), consumerHeads);

    info.GetReturnValue().Set(ret);
  }

private:
  TopicWrap(const char* path, const char* topicName) : _handle(EnvManager::getEnv(path)->getTopic(topicName)) {

  }

private:
  Topic *_handle;
};

void init(v8::Local<v8::Object> exports) {
  exports->Set(Nan::New("STRING_TYPE").ToLocalChecked(), Nan::New(STRING_TYPE));
  exports->Set(Nan::New("BUFFER_TYPE").ToLocalChecked(), Nan::New(BUFFER_TYPE));
  TopicWrap::setup(exports);
  ProducerWrap::setup(exports);
  ConsumerWrap::setup(exports);
}

NODE_MODULE(lmdb_queue, init);
