// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.3
// 	protoc        v6.31.0
// source: x/errors.proto

package monsterax

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ErrorCode int32

const (
	ErrorCode_INVALID            ErrorCode = 0
	ErrorCode_OK                 ErrorCode = 1
	ErrorCode_INVALID_ARGUMENT   ErrorCode = 2
	ErrorCode_DEADLINE_EXCEEDED  ErrorCode = 3
	ErrorCode_NOT_FOUND          ErrorCode = 4
	ErrorCode_ALREADY_EXISTS     ErrorCode = 5
	ErrorCode_RESOURCE_EXHAUSTED ErrorCode = 6
	ErrorCode_UNIMPLEMENTED      ErrorCode = 7
	ErrorCode_INTERNAL           ErrorCode = 8
)

// Enum value maps for ErrorCode.
var (
	ErrorCode_name = map[int32]string{
		0: "INVALID",
		1: "OK",
		2: "INVALID_ARGUMENT",
		3: "DEADLINE_EXCEEDED",
		4: "NOT_FOUND",
		5: "ALREADY_EXISTS",
		6: "RESOURCE_EXHAUSTED",
		7: "UNIMPLEMENTED",
		8: "INTERNAL",
	}
	ErrorCode_value = map[string]int32{
		"INVALID":            0,
		"OK":                 1,
		"INVALID_ARGUMENT":   2,
		"DEADLINE_EXCEEDED":  3,
		"NOT_FOUND":          4,
		"ALREADY_EXISTS":     5,
		"RESOURCE_EXHAUSTED": 6,
		"UNIMPLEMENTED":      7,
		"INTERNAL":           8,
	}
)

func (x ErrorCode) Enum() *ErrorCode {
	p := new(ErrorCode)
	*p = x
	return p
}

func (x ErrorCode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorCode) Descriptor() protoreflect.EnumDescriptor {
	return file_x_errors_proto_enumTypes[0].Descriptor()
}

func (ErrorCode) Type() protoreflect.EnumType {
	return &file_x_errors_proto_enumTypes[0]
}

func (x ErrorCode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ErrorCode.Descriptor instead.
func (ErrorCode) EnumDescriptor() ([]byte, []int) {
	return file_x_errors_proto_rawDescGZIP(), []int{0}
}

type Error struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Code          ErrorCode              `protobuf:"varint,1,opt,name=code,proto3,enum=com.evrblk.monstera.monsterax.ErrorCode" json:"code,omitempty"`
	Message       string                 `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	Context       []*ErrorContext        `protobuf:"bytes,3,rep,name=context,proto3" json:"context,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Error) Reset() {
	*x = Error{}
	mi := &file_x_errors_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Error) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Error) ProtoMessage() {}

func (x *Error) ProtoReflect() protoreflect.Message {
	mi := &file_x_errors_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Error.ProtoReflect.Descriptor instead.
func (*Error) Descriptor() ([]byte, []int) {
	return file_x_errors_proto_rawDescGZIP(), []int{0}
}

func (x *Error) GetCode() ErrorCode {
	if x != nil {
		return x.Code
	}
	return ErrorCode_INVALID
}

func (x *Error) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *Error) GetContext() []*ErrorContext {
	if x != nil {
		return x.Context
	}
	return nil
}

type ErrorContext struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Key           string                 `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value         string                 `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ErrorContext) Reset() {
	*x = ErrorContext{}
	mi := &file_x_errors_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ErrorContext) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ErrorContext) ProtoMessage() {}

func (x *ErrorContext) ProtoReflect() protoreflect.Message {
	mi := &file_x_errors_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ErrorContext.ProtoReflect.Descriptor instead.
func (*ErrorContext) Descriptor() ([]byte, []int) {
	return file_x_errors_proto_rawDescGZIP(), []int{1}
}

func (x *ErrorContext) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *ErrorContext) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

var File_x_errors_proto protoreflect.FileDescriptor

var file_x_errors_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x78, 0x2f, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x1d, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2e, 0x6d, 0x6f, 0x6e,
	0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x78, 0x22,
	0xa6, 0x01, 0x0a, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x3c, 0x0a, 0x04, 0x63, 0x6f, 0x64,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x28, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76,
	0x72, 0x62, 0x6c, 0x6b, 0x2e, 0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x6d, 0x6f,
	0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x78, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64,
	0x65, 0x52, 0x04, 0x63, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x12, 0x45, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x18, 0x03, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x2b, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2e,
	0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72,
	0x61, 0x78, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x52,
	0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x22, 0x36, 0x0a, 0x0c, 0x45, 0x72, 0x72, 0x6f,
	0x72, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x2a, 0xa9, 0x01, 0x0a, 0x09, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x43, 0x6f, 0x64, 0x65, 0x12, 0x0b,
	0x0a, 0x07, 0x49, 0x4e, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x10, 0x00, 0x12, 0x06, 0x0a, 0x02, 0x4f,
	0x4b, 0x10, 0x01, 0x12, 0x14, 0x0a, 0x10, 0x49, 0x4e, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x5f, 0x41,
	0x52, 0x47, 0x55, 0x4d, 0x45, 0x4e, 0x54, 0x10, 0x02, 0x12, 0x15, 0x0a, 0x11, 0x44, 0x45, 0x41,
	0x44, 0x4c, 0x49, 0x4e, 0x45, 0x5f, 0x45, 0x58, 0x43, 0x45, 0x45, 0x44, 0x45, 0x44, 0x10, 0x03,
	0x12, 0x0d, 0x0a, 0x09, 0x4e, 0x4f, 0x54, 0x5f, 0x46, 0x4f, 0x55, 0x4e, 0x44, 0x10, 0x04, 0x12,
	0x12, 0x0a, 0x0e, 0x41, 0x4c, 0x52, 0x45, 0x41, 0x44, 0x59, 0x5f, 0x45, 0x58, 0x49, 0x53, 0x54,
	0x53, 0x10, 0x05, 0x12, 0x16, 0x0a, 0x12, 0x52, 0x45, 0x53, 0x4f, 0x55, 0x52, 0x43, 0x45, 0x5f,
	0x45, 0x58, 0x48, 0x41, 0x55, 0x53, 0x54, 0x45, 0x44, 0x10, 0x06, 0x12, 0x11, 0x0a, 0x0d, 0x55,
	0x4e, 0x49, 0x4d, 0x50, 0x4c, 0x45, 0x4d, 0x45, 0x4e, 0x54, 0x45, 0x44, 0x10, 0x07, 0x12, 0x0c,
	0x0a, 0x08, 0x49, 0x4e, 0x54, 0x45, 0x52, 0x4e, 0x41, 0x4c, 0x10, 0x08, 0x42, 0x28, 0x5a, 0x26,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x65, 0x76, 0x72, 0x62, 0x6c,
	0x6b, 0x2f, 0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x2f, 0x78, 0x3b, 0x6d, 0x6f, 0x6e,
	0x73, 0x74, 0x65, 0x72, 0x61, 0x78, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_x_errors_proto_rawDescOnce sync.Once
	file_x_errors_proto_rawDescData = file_x_errors_proto_rawDesc
)

func file_x_errors_proto_rawDescGZIP() []byte {
	file_x_errors_proto_rawDescOnce.Do(func() {
		file_x_errors_proto_rawDescData = protoimpl.X.CompressGZIP(file_x_errors_proto_rawDescData)
	})
	return file_x_errors_proto_rawDescData
}

var file_x_errors_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_x_errors_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_x_errors_proto_goTypes = []any{
	(ErrorCode)(0),       // 0: com.evrblk.monstera.monsterax.ErrorCode
	(*Error)(nil),        // 1: com.evrblk.monstera.monsterax.Error
	(*ErrorContext)(nil), // 2: com.evrblk.monstera.monsterax.ErrorContext
}
var file_x_errors_proto_depIdxs = []int32{
	0, // 0: com.evrblk.monstera.monsterax.Error.code:type_name -> com.evrblk.monstera.monsterax.ErrorCode
	2, // 1: com.evrblk.monstera.monsterax.Error.context:type_name -> com.evrblk.monstera.monsterax.ErrorContext
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_x_errors_proto_init() }
func file_x_errors_proto_init() {
	if File_x_errors_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_x_errors_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_x_errors_proto_goTypes,
		DependencyIndexes: file_x_errors_proto_depIdxs,
		EnumInfos:         file_x_errors_proto_enumTypes,
		MessageInfos:      file_x_errors_proto_msgTypes,
	}.Build()
	File_x_errors_proto = out.File
	file_x_errors_proto_rawDesc = nil
	file_x_errors_proto_goTypes = nil
	file_x_errors_proto_depIdxs = nil
}
