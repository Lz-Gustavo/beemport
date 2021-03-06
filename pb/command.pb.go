// Code generated by protoc-gen-go. DO NOT EDIT.
// source: command.proto

package pb

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
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

type Command_Operation int32

const (
	Command_GET    Command_Operation = 0
	Command_SET    Command_Operation = 1
	Command_DELETE Command_Operation = 2
)

var Command_Operation_name = map[int32]string{
	0: "GET",
	1: "SET",
	2: "DELETE",
}

var Command_Operation_value = map[string]int32{
	"GET":    0,
	"SET":    1,
	"DELETE": 2,
}

func (x Command_Operation) String() string {
	return proto.EnumName(Command_Operation_name, int32(x))
}

func (Command_Operation) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_213c0bb044472049, []int{0, 0}
}

type Command struct {
	Id                   uint64            `protobuf:"varint,1,opt,name=Id,proto3" json:"Id,omitempty"`
	Ip                   string            `protobuf:"bytes,2,opt,name=Ip,proto3" json:"Ip,omitempty"`
	Op                   Command_Operation `protobuf:"varint,3,opt,name=Op,proto3,enum=pb.Command_Operation" json:"Op,omitempty"`
	Key                  string            `protobuf:"bytes,4,opt,name=Key,proto3" json:"Key,omitempty"`
	Value                string            `protobuf:"bytes,5,opt,name=Value,proto3" json:"Value,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *Command) Reset()         { *m = Command{} }
func (m *Command) String() string { return proto.CompactTextString(m) }
func (*Command) ProtoMessage()    {}
func (*Command) Descriptor() ([]byte, []int) {
	return fileDescriptor_213c0bb044472049, []int{0}
}

func (m *Command) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Command.Unmarshal(m, b)
}
func (m *Command) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Command.Marshal(b, m, deterministic)
}
func (m *Command) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Command.Merge(m, src)
}
func (m *Command) XXX_Size() int {
	return xxx_messageInfo_Command.Size(m)
}
func (m *Command) XXX_DiscardUnknown() {
	xxx_messageInfo_Command.DiscardUnknown(m)
}

var xxx_messageInfo_Command proto.InternalMessageInfo

func (m *Command) GetId() uint64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *Command) GetIp() string {
	if m != nil {
		return m.Ip
	}
	return ""
}

func (m *Command) GetOp() Command_Operation {
	if m != nil {
		return m.Op
	}
	return Command_GET
}

func (m *Command) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

func (m *Command) GetValue() string {
	if m != nil {
		return m.Value
	}
	return ""
}

func init() {
	proto.RegisterEnum("pb.Command_Operation", Command_Operation_name, Command_Operation_value)
	proto.RegisterType((*Command)(nil), "pb.Command")
}

func init() { proto.RegisterFile("command.proto", fileDescriptor_213c0bb044472049) }

var fileDescriptor_213c0bb044472049 = []byte{
	// 179 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x4d, 0xce, 0xcf, 0xcd,
	0x4d, 0xcc, 0x4b, 0xd1, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x2a, 0x48, 0x52, 0x5a, 0xcc,
	0xc8, 0xc5, 0xee, 0x0c, 0x11, 0x15, 0xe2, 0xe3, 0x62, 0xf2, 0x4c, 0x91, 0x60, 0x54, 0x60, 0xd4,
	0x60, 0x09, 0x62, 0xf2, 0x84, 0xf0, 0x0b, 0x24, 0x98, 0x14, 0x18, 0x35, 0x38, 0x83, 0x98, 0x3c,
	0x0b, 0x84, 0x54, 0xb9, 0x98, 0xfc, 0x0b, 0x24, 0x98, 0x15, 0x18, 0x35, 0xf8, 0x8c, 0x44, 0xf5,
	0x0a, 0x92, 0xf4, 0xa0, 0x1a, 0xf5, 0xfc, 0x0b, 0x52, 0x8b, 0x12, 0x4b, 0x32, 0xf3, 0xf3, 0x82,
	0x98, 0xfc, 0x0b, 0x84, 0x04, 0xb8, 0x98, 0xbd, 0x53, 0x2b, 0x25, 0x58, 0xc0, 0xfa, 0x40, 0x4c,
	0x21, 0x11, 0x2e, 0xd6, 0xb0, 0xc4, 0x9c, 0xd2, 0x54, 0x09, 0x56, 0xb0, 0x18, 0x84, 0xa3, 0xa4,
	0xc9, 0xc5, 0x09, 0xd7, 0x28, 0xc4, 0xce, 0xc5, 0xec, 0xee, 0x1a, 0x22, 0xc0, 0x00, 0x62, 0x04,
	0xbb, 0x86, 0x08, 0x30, 0x0a, 0x71, 0x71, 0xb1, 0xb9, 0xb8, 0xfa, 0xb8, 0x86, 0xb8, 0x0a, 0x30,
	0x25, 0xb1, 0x81, 0x1d, 0x6c, 0x0c, 0x08, 0x00, 0x00, 0xff, 0xff, 0x80, 0xfe, 0x3b, 0x92, 0xc1,
	0x00, 0x00, 0x00,
}
