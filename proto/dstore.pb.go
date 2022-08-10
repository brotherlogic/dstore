// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0
// 	protoc        v3.6.1
// source: dstore.proto

package proto

import (
	proto "github.com/golang/protobuf/proto"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type ReadRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Hash     string `protobuf:"bytes,2,opt,name=hash,proto3" json:"hash,omitempty"`
	NoFanout bool   `protobuf:"varint,3,opt,name=no_fanout,json=noFanout,proto3" json:"no_fanout,omitempty"`
}

func (x *ReadRequest) Reset() {
	*x = ReadRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dstore_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadRequest) ProtoMessage() {}

func (x *ReadRequest) ProtoReflect() protoreflect.Message {
	mi := &file_dstore_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadRequest.ProtoReflect.Descriptor instead.
func (*ReadRequest) Descriptor() ([]byte, []int) {
	return file_dstore_proto_rawDescGZIP(), []int{0}
}

func (x *ReadRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *ReadRequest) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

func (x *ReadRequest) GetNoFanout() bool {
	if x != nil {
		return x.NoFanout
	}
	return false
}

type ReadResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value     *anypb.Any `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	Hash      string     `protobuf:"bytes,2,opt,name=hash,proto3" json:"hash,omitempty"`
	Timestamp int64      `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Consensus float32    `protobuf:"fixed32,4,opt,name=consensus,proto3" json:"consensus,omitempty"`
}

func (x *ReadResponse) Reset() {
	*x = ReadResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dstore_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadResponse) ProtoMessage() {}

func (x *ReadResponse) ProtoReflect() protoreflect.Message {
	mi := &file_dstore_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadResponse.ProtoReflect.Descriptor instead.
func (*ReadResponse) Descriptor() ([]byte, []int) {
	return file_dstore_proto_rawDescGZIP(), []int{1}
}

func (x *ReadResponse) GetValue() *anypb.Any {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *ReadResponse) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

func (x *ReadResponse) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *ReadResponse) GetConsensus() float32 {
	if x != nil {
		return x.Consensus
	}
	return 0
}

type WriteRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key      string     `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value    *anypb.Any `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	NoFanout bool       `protobuf:"varint,3,opt,name=no_fanout,json=noFanout,proto3" json:"no_fanout,omitempty"`
}

func (x *WriteRequest) Reset() {
	*x = WriteRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dstore_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteRequest) ProtoMessage() {}

func (x *WriteRequest) ProtoReflect() protoreflect.Message {
	mi := &file_dstore_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteRequest.ProtoReflect.Descriptor instead.
func (*WriteRequest) Descriptor() ([]byte, []int) {
	return file_dstore_proto_rawDescGZIP(), []int{2}
}

func (x *WriteRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *WriteRequest) GetValue() *anypb.Any {
	if x != nil {
		return x.Value
	}
	return nil
}

func (x *WriteRequest) GetNoFanout() bool {
	if x != nil {
		return x.NoFanout
	}
	return false
}

type WriteResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Consensus float32 `protobuf:"fixed32,1,opt,name=consensus,proto3" json:"consensus,omitempty"`
	Timestamp int64   `protobuf:"varint,2,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	Hash      string  `protobuf:"bytes,3,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (x *WriteResponse) Reset() {
	*x = WriteResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dstore_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteResponse) ProtoMessage() {}

func (x *WriteResponse) ProtoReflect() protoreflect.Message {
	mi := &file_dstore_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteResponse.ProtoReflect.Descriptor instead.
func (*WriteResponse) Descriptor() ([]byte, []int) {
	return file_dstore_proto_rawDescGZIP(), []int{3}
}

func (x *WriteResponse) GetConsensus() float32 {
	if x != nil {
		return x.Consensus
	}
	return 0
}

func (x *WriteResponse) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

func (x *WriteResponse) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

type GetLatestRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key  string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Hash string `protobuf:"bytes,2,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (x *GetLatestRequest) Reset() {
	*x = GetLatestRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dstore_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetLatestRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetLatestRequest) ProtoMessage() {}

func (x *GetLatestRequest) ProtoReflect() protoreflect.Message {
	mi := &file_dstore_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetLatestRequest.ProtoReflect.Descriptor instead.
func (*GetLatestRequest) Descriptor() ([]byte, []int) {
	return file_dstore_proto_rawDescGZIP(), []int{4}
}

func (x *GetLatestRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *GetLatestRequest) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

type GetLatestResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Hash string `protobuf:"bytes,1,opt,name=hash,proto3" json:"hash,omitempty"`
}

func (x *GetLatestResponse) Reset() {
	*x = GetLatestResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dstore_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetLatestResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetLatestResponse) ProtoMessage() {}

func (x *GetLatestResponse) ProtoReflect() protoreflect.Message {
	mi := &file_dstore_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetLatestResponse.ProtoReflect.Descriptor instead.
func (*GetLatestResponse) Descriptor() ([]byte, []int) {
	return file_dstore_proto_rawDescGZIP(), []int{5}
}

func (x *GetLatestResponse) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

var File_dstore_proto protoreflect.FileDescriptor

var file_dstore_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x1a, 0x41, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x62, 0x75, 0x66, 0x66, 0x65,
	0x72, 0x73, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x73, 0x72, 0x63, 0x2f,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x50, 0x0a, 0x0b, 0x52, 0x65, 0x61,
	0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61,
	0x73, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x1b,
	0x0a, 0x09, 0x6e, 0x6f, 0x5f, 0x66, 0x61, 0x6e, 0x6f, 0x75, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x08, 0x52, 0x08, 0x6e, 0x6f, 0x46, 0x61, 0x6e, 0x6f, 0x75, 0x74, 0x22, 0x8a, 0x01, 0x0a, 0x0c,
	0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2a, 0x0a, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e,
	0x79, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x1c, 0x0a, 0x09,
	0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x1c, 0x0a, 0x09, 0x63, 0x6f,
	0x6e, 0x73, 0x65, 0x6e, 0x73, 0x75, 0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x02, 0x52, 0x09, 0x63,
	0x6f, 0x6e, 0x73, 0x65, 0x6e, 0x73, 0x75, 0x73, 0x22, 0x69, 0x0a, 0x0c, 0x57, 0x72, 0x69, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2a, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x6e, 0x6f, 0x5f, 0x66, 0x61, 0x6e,
	0x6f, 0x75, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x08, 0x6e, 0x6f, 0x46, 0x61, 0x6e,
	0x6f, 0x75, 0x74, 0x22, 0x5f, 0x0a, 0x0d, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x63, 0x6f, 0x6e, 0x73, 0x65, 0x6e, 0x73, 0x75,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x02, 0x52, 0x09, 0x63, 0x6f, 0x6e, 0x73, 0x65, 0x6e, 0x73,
	0x75, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x68, 0x61, 0x73, 0x68, 0x22, 0x38, 0x0a, 0x10, 0x47, 0x65, 0x74, 0x4c, 0x61, 0x74, 0x65, 0x73,
	0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61,
	0x73, 0x68, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x22, 0x27,
	0x0a, 0x11, 0x47, 0x65, 0x74, 0x4c, 0x61, 0x74, 0x65, 0x73, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x32, 0xc0, 0x01, 0x0a, 0x0d, 0x44, 0x53, 0x74, 0x6f,
	0x72, 0x65, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x33, 0x0a, 0x04, 0x52, 0x65, 0x61,
	0x64, 0x12, 0x13, 0x2e, 0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x14, 0x2e, 0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e,
	0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x36,
	0x0a, 0x05, 0x57, 0x72, 0x69, 0x74, 0x65, 0x12, 0x14, 0x2e, 0x64, 0x73, 0x74, 0x6f, 0x72, 0x65,
	0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e,
	0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x42, 0x0a, 0x09, 0x47, 0x65, 0x74, 0x4c, 0x61, 0x74,
	0x65, 0x73, 0x74, 0x12, 0x18, 0x2e, 0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x47, 0x65, 0x74,
	0x4c, 0x61, 0x74, 0x65, 0x73, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19, 0x2e,
	0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x47, 0x65, 0x74, 0x4c, 0x61, 0x74, 0x65, 0x73, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x26, 0x5a, 0x24, 0x67, 0x69,
	0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x62, 0x72, 0x6f, 0x74, 0x68, 0x65, 0x72,
	0x6c, 0x6f, 0x67, 0x69, 0x63, 0x2f, 0x64, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_dstore_proto_rawDescOnce sync.Once
	file_dstore_proto_rawDescData = file_dstore_proto_rawDesc
)

func file_dstore_proto_rawDescGZIP() []byte {
	file_dstore_proto_rawDescOnce.Do(func() {
		file_dstore_proto_rawDescData = protoimpl.X.CompressGZIP(file_dstore_proto_rawDescData)
	})
	return file_dstore_proto_rawDescData
}

var file_dstore_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_dstore_proto_goTypes = []interface{}{
	(*ReadRequest)(nil),       // 0: dstore.ReadRequest
	(*ReadResponse)(nil),      // 1: dstore.ReadResponse
	(*WriteRequest)(nil),      // 2: dstore.WriteRequest
	(*WriteResponse)(nil),     // 3: dstore.WriteResponse
	(*GetLatestRequest)(nil),  // 4: dstore.GetLatestRequest
	(*GetLatestResponse)(nil), // 5: dstore.GetLatestResponse
	(*anypb.Any)(nil),         // 6: google.protobuf.Any
}
var file_dstore_proto_depIdxs = []int32{
	6, // 0: dstore.ReadResponse.value:type_name -> google.protobuf.Any
	6, // 1: dstore.WriteRequest.value:type_name -> google.protobuf.Any
	0, // 2: dstore.DStoreService.Read:input_type -> dstore.ReadRequest
	2, // 3: dstore.DStoreService.Write:input_type -> dstore.WriteRequest
	4, // 4: dstore.DStoreService.GetLatest:input_type -> dstore.GetLatestRequest
	1, // 5: dstore.DStoreService.Read:output_type -> dstore.ReadResponse
	3, // 6: dstore.DStoreService.Write:output_type -> dstore.WriteResponse
	5, // 7: dstore.DStoreService.GetLatest:output_type -> dstore.GetLatestResponse
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_dstore_proto_init() }
func file_dstore_proto_init() {
	if File_dstore_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_dstore_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dstore_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dstore_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dstore_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dstore_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetLatestRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dstore_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetLatestResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_dstore_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_dstore_proto_goTypes,
		DependencyIndexes: file_dstore_proto_depIdxs,
		MessageInfos:      file_dstore_proto_msgTypes,
	}.Build()
	File_dstore_proto = out.File
	file_dstore_proto_rawDesc = nil
	file_dstore_proto_goTypes = nil
	file_dstore_proto_depIdxs = nil
}
