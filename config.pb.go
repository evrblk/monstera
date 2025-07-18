// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.3
// 	protoc        v6.31.0
// source: config.proto

package monstera

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

type Node struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Address       string                 `protobuf:"bytes,2,opt,name=address,proto3" json:"address,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Node) Reset() {
	*x = Node{}
	mi := &file_config_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Node) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Node) ProtoMessage() {}

func (x *Node) ProtoReflect() protoreflect.Message {
	mi := &file_config_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Node.ProtoReflect.Descriptor instead.
func (*Node) Descriptor() ([]byte, []int) {
	return file_config_proto_rawDescGZIP(), []int{0}
}

func (x *Node) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Node) GetAddress() string {
	if x != nil {
		return x.Address
	}
	return ""
}

type Application struct {
	state             protoimpl.MessageState `protogen:"open.v1"`
	Name              string                 `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Implementation    string                 `protobuf:"bytes,2,opt,name=implementation,proto3" json:"implementation,omitempty"`
	Shards            []*Shard               `protobuf:"bytes,3,rep,name=shards,proto3" json:"shards,omitempty"`
	ReplicationFactor int32                  `protobuf:"varint,4,opt,name=replication_factor,json=replicationFactor,proto3" json:"replication_factor,omitempty"`
	unknownFields     protoimpl.UnknownFields
	sizeCache         protoimpl.SizeCache
}

func (x *Application) Reset() {
	*x = Application{}
	mi := &file_config_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Application) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Application) ProtoMessage() {}

func (x *Application) ProtoReflect() protoreflect.Message {
	mi := &file_config_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Application.ProtoReflect.Descriptor instead.
func (*Application) Descriptor() ([]byte, []int) {
	return file_config_proto_rawDescGZIP(), []int{1}
}

func (x *Application) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Application) GetImplementation() string {
	if x != nil {
		return x.Implementation
	}
	return ""
}

func (x *Application) GetShards() []*Shard {
	if x != nil {
		return x.Shards
	}
	return nil
}

func (x *Application) GetReplicationFactor() int32 {
	if x != nil {
		return x.ReplicationFactor
	}
	return 0
}

type Shard struct {
	state             protoimpl.MessageState `protogen:"open.v1"`
	Id                string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	LowerBound        []byte                 `protobuf:"bytes,2,opt,name=lower_bound,json=lowerBound,proto3" json:"lower_bound,omitempty"`
	UpperBound        []byte                 `protobuf:"bytes,3,opt,name=upper_bound,json=upperBound,proto3" json:"upper_bound,omitempty"`
	GlobalIndexPrefix []byte                 `protobuf:"bytes,4,opt,name=global_index_prefix,json=globalIndexPrefix,proto3" json:"global_index_prefix,omitempty"`
	ParentId          string                 `protobuf:"bytes,5,opt,name=parent_id,json=parentId,proto3" json:"parent_id,omitempty"`
	Replicas          []*Replica             `protobuf:"bytes,6,rep,name=replicas,proto3" json:"replicas,omitempty"`
	unknownFields     protoimpl.UnknownFields
	sizeCache         protoimpl.SizeCache
}

func (x *Shard) Reset() {
	*x = Shard{}
	mi := &file_config_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Shard) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Shard) ProtoMessage() {}

func (x *Shard) ProtoReflect() protoreflect.Message {
	mi := &file_config_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Shard.ProtoReflect.Descriptor instead.
func (*Shard) Descriptor() ([]byte, []int) {
	return file_config_proto_rawDescGZIP(), []int{2}
}

func (x *Shard) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Shard) GetLowerBound() []byte {
	if x != nil {
		return x.LowerBound
	}
	return nil
}

func (x *Shard) GetUpperBound() []byte {
	if x != nil {
		return x.UpperBound
	}
	return nil
}

func (x *Shard) GetGlobalIndexPrefix() []byte {
	if x != nil {
		return x.GlobalIndexPrefix
	}
	return nil
}

func (x *Shard) GetParentId() string {
	if x != nil {
		return x.ParentId
	}
	return ""
}

func (x *Shard) GetReplicas() []*Replica {
	if x != nil {
		return x.Replicas
	}
	return nil
}

type Replica struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Id            string                 `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	NodeId        string                 `protobuf:"bytes,2,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Replica) Reset() {
	*x = Replica{}
	mi := &file_config_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Replica) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Replica) ProtoMessage() {}

func (x *Replica) ProtoReflect() protoreflect.Message {
	mi := &file_config_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Replica.ProtoReflect.Descriptor instead.
func (*Replica) Descriptor() ([]byte, []int) {
	return file_config_proto_rawDescGZIP(), []int{3}
}

func (x *Replica) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Replica) GetNodeId() string {
	if x != nil {
		return x.NodeId
	}
	return ""
}

type ClusterConfig struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Applications  []*Application         `protobuf:"bytes,1,rep,name=applications,proto3" json:"applications,omitempty"`
	Nodes         []*Node                `protobuf:"bytes,2,rep,name=nodes,proto3" json:"nodes,omitempty"`
	UpdatedAt     int64                  `protobuf:"varint,3,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *ClusterConfig) Reset() {
	*x = ClusterConfig{}
	mi := &file_config_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ClusterConfig) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ClusterConfig) ProtoMessage() {}

func (x *ClusterConfig) ProtoReflect() protoreflect.Message {
	mi := &file_config_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ClusterConfig.ProtoReflect.Descriptor instead.
func (*ClusterConfig) Descriptor() ([]byte, []int) {
	return file_config_proto_rawDescGZIP(), []int{4}
}

func (x *ClusterConfig) GetApplications() []*Application {
	if x != nil {
		return x.Applications
	}
	return nil
}

func (x *ClusterConfig) GetNodes() []*Node {
	if x != nil {
		return x.Nodes
	}
	return nil
}

func (x *ClusterConfig) GetUpdatedAt() int64 {
	if x != nil {
		return x.UpdatedAt
	}
	return 0
}

var File_config_proto protoreflect.FileDescriptor

var file_config_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x13,
	0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2e, 0x6d, 0x6f, 0x6e, 0x73, 0x74,
	0x65, 0x72, 0x61, 0x22, 0x30, 0x0a, 0x04, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x61,
	0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x61, 0x64,
	0x64, 0x72, 0x65, 0x73, 0x73, 0x22, 0xac, 0x01, 0x0a, 0x0b, 0x41, 0x70, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x26, 0x0a, 0x0e, 0x69, 0x6d, 0x70,
	0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0e, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x32, 0x0a, 0x06, 0x73, 0x68, 0x61, 0x72, 0x64, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x1a, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2e, 0x6d,
	0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x53, 0x68, 0x61, 0x72, 0x64, 0x52, 0x06, 0x73,
	0x68, 0x61, 0x72, 0x64, 0x73, 0x12, 0x2d, 0x0a, 0x12, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x66, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x05, 0x52, 0x11, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x46, 0x61,
	0x63, 0x74, 0x6f, 0x72, 0x22, 0xe0, 0x01, 0x0a, 0x05, 0x53, 0x68, 0x61, 0x72, 0x64, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x1f,
	0x0a, 0x0b, 0x6c, 0x6f, 0x77, 0x65, 0x72, 0x5f, 0x62, 0x6f, 0x75, 0x6e, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x0a, 0x6c, 0x6f, 0x77, 0x65, 0x72, 0x42, 0x6f, 0x75, 0x6e, 0x64, 0x12,
	0x1f, 0x0a, 0x0b, 0x75, 0x70, 0x70, 0x65, 0x72, 0x5f, 0x62, 0x6f, 0x75, 0x6e, 0x64, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x0a, 0x75, 0x70, 0x70, 0x65, 0x72, 0x42, 0x6f, 0x75, 0x6e, 0x64,
	0x12, 0x2e, 0x0a, 0x13, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78,
	0x5f, 0x70, 0x72, 0x65, 0x66, 0x69, 0x78, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x11, 0x67,
	0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x50, 0x72, 0x65, 0x66, 0x69, 0x78,
	0x12, 0x1b, 0x0a, 0x09, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x69, 0x64, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x08, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x49, 0x64, 0x12, 0x38, 0x0a,
	0x08, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x1c, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2e, 0x6d, 0x6f, 0x6e,
	0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x52, 0x08, 0x72,
	0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x73, 0x22, 0x32, 0x0a, 0x07, 0x52, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02,
	0x69, 0x64, 0x12, 0x17, 0x0a, 0x07, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x06, 0x6e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x22, 0xa5, 0x01, 0x0a, 0x0d,
	0x43, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x12, 0x44, 0x0a,
	0x0c, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b,
	0x2e, 0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x41, 0x70, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0c, 0x61, 0x70, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x73, 0x12, 0x2f, 0x0a, 0x05, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x19, 0x2e, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2e,
	0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72, 0x61, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x52, 0x05, 0x6e,
	0x6f, 0x64, 0x65, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x64, 0x5f,
	0x61, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x64, 0x41, 0x74, 0x42, 0x1c, 0x5a, 0x1a, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x65, 0x76, 0x72, 0x62, 0x6c, 0x6b, 0x2f, 0x6d, 0x6f, 0x6e, 0x73, 0x74, 0x65, 0x72,
	0x61, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_config_proto_rawDescOnce sync.Once
	file_config_proto_rawDescData = file_config_proto_rawDesc
)

func file_config_proto_rawDescGZIP() []byte {
	file_config_proto_rawDescOnce.Do(func() {
		file_config_proto_rawDescData = protoimpl.X.CompressGZIP(file_config_proto_rawDescData)
	})
	return file_config_proto_rawDescData
}

var file_config_proto_msgTypes = make([]protoimpl.MessageInfo, 5)
var file_config_proto_goTypes = []any{
	(*Node)(nil),          // 0: com.evrblk.monstera.Node
	(*Application)(nil),   // 1: com.evrblk.monstera.Application
	(*Shard)(nil),         // 2: com.evrblk.monstera.Shard
	(*Replica)(nil),       // 3: com.evrblk.monstera.Replica
	(*ClusterConfig)(nil), // 4: com.evrblk.monstera.ClusterConfig
}
var file_config_proto_depIdxs = []int32{
	2, // 0: com.evrblk.monstera.Application.shards:type_name -> com.evrblk.monstera.Shard
	3, // 1: com.evrblk.monstera.Shard.replicas:type_name -> com.evrblk.monstera.Replica
	1, // 2: com.evrblk.monstera.ClusterConfig.applications:type_name -> com.evrblk.monstera.Application
	0, // 3: com.evrblk.monstera.ClusterConfig.nodes:type_name -> com.evrblk.monstera.Node
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_config_proto_init() }
func file_config_proto_init() {
	if File_config_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_config_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   5,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_config_proto_goTypes,
		DependencyIndexes: file_config_proto_depIdxs,
		MessageInfos:      file_config_proto_msgTypes,
	}.Build()
	File_config_proto = out.File
	file_config_proto_rawDesc = nil
	file_config_proto_goTypes = nil
	file_config_proto_depIdxs = nil
}
