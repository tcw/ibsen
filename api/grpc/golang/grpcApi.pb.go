// Code generated by protoc-gen-go. DO NOT EDIT.
// source: grpcApi.proto

package grpcApi

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Empty struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Empty) Reset()         { *m = Empty{} }
func (m *Empty) String() string { return proto.CompactTextString(m) }
func (*Empty) ProtoMessage()    {}
func (*Empty) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{0}
}

func (m *Empty) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Empty.Unmarshal(m, b)
}
func (m *Empty) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Empty.Marshal(b, m, deterministic)
}
func (m *Empty) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Empty.Merge(m, src)
}
func (m *Empty) XXX_Size() int {
	return xxx_messageInfo_Empty.Size(m)
}
func (m *Empty) XXX_DiscardUnknown() {
	xxx_messageInfo_Empty.DiscardUnknown(m)
}

var xxx_messageInfo_Empty proto.InternalMessageInfo

type Offset struct {
	Id                   uint64   `protobuf:"varint,1,opt,name=id,proto3" json:"id,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Offset) Reset()         { *m = Offset{} }
func (m *Offset) String() string { return proto.CompactTextString(m) }
func (*Offset) ProtoMessage()    {}
func (*Offset) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{1}
}

func (m *Offset) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Offset.Unmarshal(m, b)
}
func (m *Offset) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Offset.Marshal(b, m, deterministic)
}
func (m *Offset) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Offset.Merge(m, src)
}
func (m *Offset) XXX_Size() int {
	return xxx_messageInfo_Offset.Size(m)
}
func (m *Offset) XXX_DiscardUnknown() {
	xxx_messageInfo_Offset.DiscardUnknown(m)
}

var xxx_messageInfo_Offset proto.InternalMessageInfo

func (m *Offset) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

type Topic struct {
	Name                 string   `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Topic) Reset()         { *m = Topic{} }
func (m *Topic) String() string { return proto.CompactTextString(m) }
func (*Topic) ProtoMessage()    {}
func (*Topic) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{2}
}

func (m *Topic) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Topic.Unmarshal(m, b)
}
func (m *Topic) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Topic.Marshal(b, m, deterministic)
}
func (m *Topic) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Topic.Merge(m, src)
}
func (m *Topic) XXX_Size() int {
	return xxx_messageInfo_Topic.Size(m)
}
func (m *Topic) XXX_DiscardUnknown() {
	xxx_messageInfo_Topic.DiscardUnknown(m)
}

var xxx_messageInfo_Topic proto.InternalMessageInfo

func (m *Topic) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type TopicOffset struct {
	TopicName            string   `protobuf:"bytes,1,opt,name=topicName,proto3" json:"topicName,omitempty"`
	Offset               uint64   `protobuf:"varint,2,opt,name=offset,proto3" json:"offset,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *TopicOffset) Reset()         { *m = TopicOffset{} }
func (m *TopicOffset) String() string { return proto.CompactTextString(m) }
func (*TopicOffset) ProtoMessage()    {}
func (*TopicOffset) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{3}
}

func (m *TopicOffset) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TopicOffset.Unmarshal(m, b)
}
func (m *TopicOffset) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TopicOffset.Marshal(b, m, deterministic)
}
func (m *TopicOffset) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TopicOffset.Merge(m, src)
}
func (m *TopicOffset) XXX_Size() int {
	return xxx_messageInfo_TopicOffset.Size(m)
}
func (m *TopicOffset) XXX_DiscardUnknown() {
	xxx_messageInfo_TopicOffset.DiscardUnknown(m)
}

var xxx_messageInfo_TopicOffset proto.InternalMessageInfo

func (m *TopicOffset) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

func (m *TopicOffset) GetOffset() uint64 {
	if m != nil {
		return m.Offset
	}
	return 0
}

type TopicMessage struct {
	TopicName            string   `protobuf:"bytes,1,opt,name=topicName,proto3" json:"topicName,omitempty"`
	MessagePayload       []byte   `protobuf:"bytes,2,opt,name=messagePayload,proto3" json:"messagePayload,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *TopicMessage) Reset()         { *m = TopicMessage{} }
func (m *TopicMessage) String() string { return proto.CompactTextString(m) }
func (*TopicMessage) ProtoMessage()    {}
func (*TopicMessage) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{4}
}

func (m *TopicMessage) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_TopicMessage.Unmarshal(m, b)
}
func (m *TopicMessage) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_TopicMessage.Marshal(b, m, deterministic)
}
func (m *TopicMessage) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TopicMessage.Merge(m, src)
}
func (m *TopicMessage) XXX_Size() int {
	return xxx_messageInfo_TopicMessage.Size(m)
}
func (m *TopicMessage) XXX_DiscardUnknown() {
	xxx_messageInfo_TopicMessage.DiscardUnknown(m)
}

var xxx_messageInfo_TopicMessage proto.InternalMessageInfo

func (m *TopicMessage) GetTopicName() string {
	if m != nil {
		return m.TopicName
	}
	return ""
}

func (m *TopicMessage) GetMessagePayload() []byte {
	if m != nil {
		return m.MessagePayload
	}
	return nil
}

type Message struct {
	Payload              []byte   `protobuf:"bytes,1,opt,name=payload,proto3" json:"payload,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Message) Reset()         { *m = Message{} }
func (m *Message) String() string { return proto.CompactTextString(m) }
func (*Message) ProtoMessage()    {}
func (*Message) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{5}
}

func (m *Message) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Message.Unmarshal(m, b)
}
func (m *Message) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Message.Marshal(b, m, deterministic)
}
func (m *Message) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Message.Merge(m, src)
}
func (m *Message) XXX_Size() int {
	return xxx_messageInfo_Message.Size(m)
}
func (m *Message) XXX_DiscardUnknown() {
	xxx_messageInfo_Message.DiscardUnknown(m)
}

var xxx_messageInfo_Message proto.InternalMessageInfo

func (m *Message) GetPayload() []byte {
	if m != nil {
		return m.Payload
	}
	return nil
}

type Status struct {
	Entries              uint64   `protobuf:"varint,1,opt,name=entries,proto3" json:"entries,omitempty"`
	Current              *Offset  `protobuf:"bytes,2,opt,name=current,proto3" json:"current,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Status) Reset()         { *m = Status{} }
func (m *Status) String() string { return proto.CompactTextString(m) }
func (*Status) ProtoMessage()    {}
func (*Status) Descriptor() ([]byte, []int) {
	return fileDescriptor_b9b08cba6a2dfae2, []int{6}
}

func (m *Status) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Status.Unmarshal(m, b)
}
func (m *Status) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Status.Marshal(b, m, deterministic)
}
func (m *Status) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Status.Merge(m, src)
}
func (m *Status) XXX_Size() int {
	return xxx_messageInfo_Status.Size(m)
}
func (m *Status) XXX_DiscardUnknown() {
	xxx_messageInfo_Status.DiscardUnknown(m)
}

var xxx_messageInfo_Status proto.InternalMessageInfo

func (m *Status) GetEntries() uint64 {
	if m != nil {
		return m.Entries
	}
	return 0
}

func (m *Status) GetCurrent() *Offset {
	if m != nil {
		return m.Current
	}
	return nil
}

func init() {
	proto.RegisterType((*Empty)(nil), "Empty")
	proto.RegisterType((*Offset)(nil), "Offset")
	proto.RegisterType((*Topic)(nil), "Topic")
	proto.RegisterType((*TopicOffset)(nil), "TopicOffset")
	proto.RegisterType((*TopicMessage)(nil), "TopicMessage")
	proto.RegisterType((*Message)(nil), "Message")
	proto.RegisterType((*Status)(nil), "Status")
}

func init() { proto.RegisterFile("grpcApi.proto", fileDescriptor_b9b08cba6a2dfae2) }

var fileDescriptor_b9b08cba6a2dfae2 = []byte{
	// 364 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x92, 0xcb, 0x6a, 0xeb, 0x30,
	0x10, 0x40, 0x63, 0x93, 0xd8, 0x37, 0x93, 0x07, 0x5c, 0x71, 0xb9, 0x18, 0xb7, 0x8b, 0x44, 0x85,
	0x92, 0x40, 0x11, 0x21, 0xfd, 0x82, 0x3e, 0xd2, 0x5d, 0x1f, 0x38, 0x81, 0xae, 0x55, 0x5b, 0x71,
	0x45, 0x63, 0x4b, 0x48, 0x4a, 0x4b, 0x3e, 0xae, 0xff, 0x56, 0x22, 0xcb, 0x4d, 0x1a, 0x5a, 0xba,
	0xf3, 0xcc, 0x9c, 0x39, 0x9e, 0x91, 0x04, 0xbd, 0x5c, 0xc9, 0xf4, 0x42, 0x72, 0x22, 0x95, 0x30,
	0x02, 0x87, 0xd0, 0x9a, 0x15, 0xd2, 0x6c, 0x70, 0x04, 0xc1, 0xfd, 0x72, 0xa9, 0x99, 0x41, 0x7d,
	0xf0, 0x79, 0x16, 0x79, 0x03, 0x6f, 0xd4, 0x4c, 0x7c, 0x9e, 0xe1, 0x23, 0x68, 0x2d, 0x84, 0xe4,
	0x29, 0x42, 0xd0, 0x2c, 0x69, 0xc1, 0x6c, 0xa9, 0x9d, 0xd8, 0x6f, 0x7c, 0x05, 0x1d, 0x5b, 0x74,
	0xbd, 0xc7, 0xd0, 0x36, 0xdb, 0xf0, 0x6e, 0xc7, 0xed, 0x12, 0xe8, 0x3f, 0x04, 0xc2, 0x72, 0x91,
	0x6f, 0xed, 0x2e, 0xc2, 0x0b, 0xe8, 0x5a, 0xc9, 0x2d, 0xd3, 0x9a, 0xe6, 0xec, 0x17, 0xcb, 0x29,
	0xf4, 0x8b, 0x0a, 0x7c, 0xa0, 0x9b, 0x95, 0xa0, 0x99, 0xb5, 0x75, 0x93, 0x83, 0x2c, 0x3e, 0x81,
	0xb0, 0x16, 0x46, 0x10, 0x4a, 0xc7, 0x7a, 0x96, 0xad, 0x43, 0x3c, 0x83, 0x60, 0x6e, 0xa8, 0x59,
	0xeb, 0x2d, 0xc3, 0x4a, 0xa3, 0x38, 0xd3, 0x6e, 0xf7, 0x3a, 0x44, 0x43, 0x08, 0xd3, 0xb5, 0x52,
	0xac, 0xac, 0xe6, 0xee, 0x4c, 0x43, 0x52, 0xad, 0x9b, 0xd4, 0xf9, 0xe9, 0xbb, 0x0f, 0x70, 0x2d,
	0xb4, 0x11, 0xec, 0x55, 0xbf, 0x6c, 0x50, 0x0c, 0x41, 0xaa, 0x18, 0x35, 0x0c, 0x05, 0xc4, 0x6e,
	0x16, 0x07, 0xa4, 0x3a, 0xe6, 0x06, 0x8a, 0xa0, 0x99, 0x29, 0x21, 0xbf, 0xa9, 0x0c, 0xa1, 0xf5,
	0xa6, 0xb8, 0x61, 0xa8, 0x47, 0xf6, 0x8f, 0x23, 0x0e, 0x49, 0x35, 0x22, 0x6e, 0xa0, 0x31, 0x74,
	0x2c, 0x32, 0x37, 0x8a, 0xd1, 0xe2, 0x67, 0x70, 0xe4, 0xa1, 0x31, 0xfc, 0x55, 0x8c, 0x66, 0x37,
	0x4a, 0x14, 0x97, 0x2c, 0xe7, 0x65, 0xc9, 0xcb, 0xfc, 0xf3, 0xa7, 0x7f, 0x88, 0xeb, 0xc1, 0x8d,
	0x89, 0x87, 0xce, 0xa0, 0x5f, 0xa3, 0xee, 0x1e, 0xbb, 0x64, 0xef, 0x56, 0x0f, 0xe8, 0x01, 0xc0,
	0x8a, 0x6b, 0x63, 0x01, 0x8d, 0xdc, 0xf8, 0xb1, 0x33, 0x5b, 0x62, 0x02, 0xff, 0x76, 0xc4, 0x23,
	0x37, 0xcf, 0xce, 0x5a, 0xb3, 0x5f, 0xec, 0xdb, 0x8e, 0xa7, 0xc0, 0xbe, 0xc6, 0xf3, 0x8f, 0x00,
	0x00, 0x00, 0xff, 0xff, 0xcc, 0x8d, 0xd6, 0xc0, 0x9e, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// DostoevskyClient is the client API for Dostoevsky service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type DostoevskyClient interface {
	Create(ctx context.Context, in *Topic, opts ...grpc.CallOption) (*Empty, error)
	Drop(ctx context.Context, in *Topic, opts ...grpc.CallOption) (*Empty, error)
	Write(ctx context.Context, in *TopicMessage, opts ...grpc.CallOption) (*Status, error)
	WriteStream(ctx context.Context, opts ...grpc.CallOption) (Dostoevsky_WriteStreamClient, error)
	ReadFromBeginning(ctx context.Context, in *Topic, opts ...grpc.CallOption) (Dostoevsky_ReadFromBeginningClient, error)
	ReadFromOffset(ctx context.Context, in *TopicOffset, opts ...grpc.CallOption) (Dostoevsky_ReadFromOffsetClient, error)
	ListTopics(ctx context.Context, in *Empty, opts ...grpc.CallOption) (Dostoevsky_ListTopicsClient, error)
	ListTopicsWithOffset(ctx context.Context, in *Empty, opts ...grpc.CallOption) (Dostoevsky_ListTopicsWithOffsetClient, error)
}

type dostoevskyClient struct {
	cc grpc.ClientConnInterface
}

func NewDostoevskyClient(cc grpc.ClientConnInterface) DostoevskyClient {
	return &dostoevskyClient{cc}
}

func (c *dostoevskyClient) Create(ctx context.Context, in *Topic, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/Dostoevsky/create", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dostoevskyClient) Drop(ctx context.Context, in *Topic, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/Dostoevsky/drop", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dostoevskyClient) Write(ctx context.Context, in *TopicMessage, opts ...grpc.CallOption) (*Status, error) {
	out := new(Status)
	err := c.cc.Invoke(ctx, "/Dostoevsky/write", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dostoevskyClient) WriteStream(ctx context.Context, opts ...grpc.CallOption) (Dostoevsky_WriteStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Dostoevsky_serviceDesc.Streams[0], "/Dostoevsky/writeStream", opts...)
	if err != nil {
		return nil, err
	}
	x := &dostoevskyWriteStreamClient{stream}
	return x, nil
}

type Dostoevsky_WriteStreamClient interface {
	Send(*TopicMessage) error
	CloseAndRecv() (*Status, error)
	grpc.ClientStream
}

type dostoevskyWriteStreamClient struct {
	grpc.ClientStream
}

func (x *dostoevskyWriteStreamClient) Send(m *TopicMessage) error {
	return x.ClientStream.SendMsg(m)
}

func (x *dostoevskyWriteStreamClient) CloseAndRecv() (*Status, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(Status)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dostoevskyClient) ReadFromBeginning(ctx context.Context, in *Topic, opts ...grpc.CallOption) (Dostoevsky_ReadFromBeginningClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Dostoevsky_serviceDesc.Streams[1], "/Dostoevsky/readFromBeginning", opts...)
	if err != nil {
		return nil, err
	}
	x := &dostoevskyReadFromBeginningClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Dostoevsky_ReadFromBeginningClient interface {
	Recv() (*Message, error)
	grpc.ClientStream
}

type dostoevskyReadFromBeginningClient struct {
	grpc.ClientStream
}

func (x *dostoevskyReadFromBeginningClient) Recv() (*Message, error) {
	m := new(Message)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dostoevskyClient) ReadFromOffset(ctx context.Context, in *TopicOffset, opts ...grpc.CallOption) (Dostoevsky_ReadFromOffsetClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Dostoevsky_serviceDesc.Streams[2], "/Dostoevsky/readFromOffset", opts...)
	if err != nil {
		return nil, err
	}
	x := &dostoevskyReadFromOffsetClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Dostoevsky_ReadFromOffsetClient interface {
	Recv() (*Message, error)
	grpc.ClientStream
}

type dostoevskyReadFromOffsetClient struct {
	grpc.ClientStream
}

func (x *dostoevskyReadFromOffsetClient) Recv() (*Message, error) {
	m := new(Message)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dostoevskyClient) ListTopics(ctx context.Context, in *Empty, opts ...grpc.CallOption) (Dostoevsky_ListTopicsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Dostoevsky_serviceDesc.Streams[3], "/Dostoevsky/listTopics", opts...)
	if err != nil {
		return nil, err
	}
	x := &dostoevskyListTopicsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Dostoevsky_ListTopicsClient interface {
	Recv() (*Topic, error)
	grpc.ClientStream
}

type dostoevskyListTopicsClient struct {
	grpc.ClientStream
}

func (x *dostoevskyListTopicsClient) Recv() (*Topic, error) {
	m := new(Topic)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dostoevskyClient) ListTopicsWithOffset(ctx context.Context, in *Empty, opts ...grpc.CallOption) (Dostoevsky_ListTopicsWithOffsetClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Dostoevsky_serviceDesc.Streams[4], "/Dostoevsky/listTopicsWithOffset", opts...)
	if err != nil {
		return nil, err
	}
	x := &dostoevskyListTopicsWithOffsetClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Dostoevsky_ListTopicsWithOffsetClient interface {
	Recv() (*TopicOffset, error)
	grpc.ClientStream
}

type dostoevskyListTopicsWithOffsetClient struct {
	grpc.ClientStream
}

func (x *dostoevskyListTopicsWithOffsetClient) Recv() (*TopicOffset, error) {
	m := new(TopicOffset)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DostoevskyServer is the server API for Dostoevsky service.
type DostoevskyServer interface {
	Create(context.Context, *Topic) (*Empty, error)
	Drop(context.Context, *Topic) (*Empty, error)
	Write(context.Context, *TopicMessage) (*Status, error)
	WriteStream(Dostoevsky_WriteStreamServer) error
	ReadFromBeginning(*Topic, Dostoevsky_ReadFromBeginningServer) error
	ReadFromOffset(*TopicOffset, Dostoevsky_ReadFromOffsetServer) error
	ListTopics(*Empty, Dostoevsky_ListTopicsServer) error
	ListTopicsWithOffset(*Empty, Dostoevsky_ListTopicsWithOffsetServer) error
}

// UnimplementedDostoevskyServer can be embedded to have forward compatible implementations.
type UnimplementedDostoevskyServer struct {
}

func (*UnimplementedDostoevskyServer) Create(ctx context.Context, req *Topic) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Create not implemented")
}
func (*UnimplementedDostoevskyServer) Drop(ctx context.Context, req *Topic) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Drop not implemented")
}
func (*UnimplementedDostoevskyServer) Write(ctx context.Context, req *TopicMessage) (*Status, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Write not implemented")
}
func (*UnimplementedDostoevskyServer) WriteStream(srv Dostoevsky_WriteStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method WriteStream not implemented")
}
func (*UnimplementedDostoevskyServer) ReadFromBeginning(req *Topic, srv Dostoevsky_ReadFromBeginningServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadFromBeginning not implemented")
}
func (*UnimplementedDostoevskyServer) ReadFromOffset(req *TopicOffset, srv Dostoevsky_ReadFromOffsetServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadFromOffset not implemented")
}
func (*UnimplementedDostoevskyServer) ListTopics(req *Empty, srv Dostoevsky_ListTopicsServer) error {
	return status.Errorf(codes.Unimplemented, "method ListTopics not implemented")
}
func (*UnimplementedDostoevskyServer) ListTopicsWithOffset(req *Empty, srv Dostoevsky_ListTopicsWithOffsetServer) error {
	return status.Errorf(codes.Unimplemented, "method ListTopicsWithOffset not implemented")
}

func RegisterDostoevskyServer(s *grpc.Server, srv DostoevskyServer) {
	s.RegisterService(&_Dostoevsky_serviceDesc, srv)
}

func _Dostoevsky_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Topic)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DostoevskyServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Dostoevsky/Create",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DostoevskyServer).Create(ctx, req.(*Topic))
	}
	return interceptor(ctx, in, info, handler)
}

func _Dostoevsky_Drop_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Topic)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DostoevskyServer).Drop(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Dostoevsky/Drop",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DostoevskyServer).Drop(ctx, req.(*Topic))
	}
	return interceptor(ctx, in, info, handler)
}

func _Dostoevsky_Write_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TopicMessage)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DostoevskyServer).Write(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/Dostoevsky/Write",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DostoevskyServer).Write(ctx, req.(*TopicMessage))
	}
	return interceptor(ctx, in, info, handler)
}

func _Dostoevsky_WriteStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DostoevskyServer).WriteStream(&dostoevskyWriteStreamServer{stream})
}

type Dostoevsky_WriteStreamServer interface {
	SendAndClose(*Status) error
	Recv() (*TopicMessage, error)
	grpc.ServerStream
}

type dostoevskyWriteStreamServer struct {
	grpc.ServerStream
}

func (x *dostoevskyWriteStreamServer) SendAndClose(m *Status) error {
	return x.ServerStream.SendMsg(m)
}

func (x *dostoevskyWriteStreamServer) Recv() (*TopicMessage, error) {
	m := new(TopicMessage)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Dostoevsky_ReadFromBeginning_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Topic)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DostoevskyServer).ReadFromBeginning(m, &dostoevskyReadFromBeginningServer{stream})
}

type Dostoevsky_ReadFromBeginningServer interface {
	Send(*Message) error
	grpc.ServerStream
}

type dostoevskyReadFromBeginningServer struct {
	grpc.ServerStream
}

func (x *dostoevskyReadFromBeginningServer) Send(m *Message) error {
	return x.ServerStream.SendMsg(m)
}

func _Dostoevsky_ReadFromOffset_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(TopicOffset)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DostoevskyServer).ReadFromOffset(m, &dostoevskyReadFromOffsetServer{stream})
}

type Dostoevsky_ReadFromOffsetServer interface {
	Send(*Message) error
	grpc.ServerStream
}

type dostoevskyReadFromOffsetServer struct {
	grpc.ServerStream
}

func (x *dostoevskyReadFromOffsetServer) Send(m *Message) error {
	return x.ServerStream.SendMsg(m)
}

func _Dostoevsky_ListTopics_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DostoevskyServer).ListTopics(m, &dostoevskyListTopicsServer{stream})
}

type Dostoevsky_ListTopicsServer interface {
	Send(*Topic) error
	grpc.ServerStream
}

type dostoevskyListTopicsServer struct {
	grpc.ServerStream
}

func (x *dostoevskyListTopicsServer) Send(m *Topic) error {
	return x.ServerStream.SendMsg(m)
}

func _Dostoevsky_ListTopicsWithOffset_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DostoevskyServer).ListTopicsWithOffset(m, &dostoevskyListTopicsWithOffsetServer{stream})
}

type Dostoevsky_ListTopicsWithOffsetServer interface {
	Send(*TopicOffset) error
	grpc.ServerStream
}

type dostoevskyListTopicsWithOffsetServer struct {
	grpc.ServerStream
}

func (x *dostoevskyListTopicsWithOffsetServer) Send(m *TopicOffset) error {
	return x.ServerStream.SendMsg(m)
}

var _Dostoevsky_serviceDesc = grpc.ServiceDesc{
	ServiceName: "Dostoevsky",
	HandlerType: (*DostoevskyServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "create",
			Handler:    _Dostoevsky_Create_Handler,
		},
		{
			MethodName: "drop",
			Handler:    _Dostoevsky_Drop_Handler,
		},
		{
			MethodName: "write",
			Handler:    _Dostoevsky_Write_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "writeStream",
			Handler:       _Dostoevsky_WriteStream_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "readFromBeginning",
			Handler:       _Dostoevsky_ReadFromBeginning_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "readFromOffset",
			Handler:       _Dostoevsky_ReadFromOffset_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "listTopics",
			Handler:       _Dostoevsky_ListTopics_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "listTopicsWithOffset",
			Handler:       _Dostoevsky_ListTopicsWithOffset_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "grpcApi.proto",
}
