package hysteria

import (
	"net"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const FlipTrigger = "BitsFlip"

func NewBitFlipPacketConn(conn net.PacketConn) net.PacketConn {
	vectorisedWriter, isVectorised := bufio.CreateVectorisedPacketWriter(conn)
	if isVectorised {
		return &VectorisedBitFlipConn{
			BitFlipPacketConn: BitFlipPacketConn{
				PacketConn: conn,
			},
			writer: vectorisedWriter,
		}
	} else {
		return &BitFlipPacketConn{
			PacketConn: conn,
		}
	}
}

type BitFlipPacketConn struct {
	net.PacketConn
}

func flipBits(p []byte) int {
	for i, c := range p {
		p[i] = ^c
	}
	return len(p)
}

func (c *BitFlipPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	n, addr, err = c.PacketConn.ReadFrom(p)
	if err != nil {
		return
	}

	flipBits(p)
	return
}

func (c *BitFlipPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	flipBits(p)
	return c.PacketConn.WriteTo(p, addr)
}

func (c *BitFlipPacketConn) Upstream() any {
	return c.PacketConn
}

type VectorisedBitFlipConn struct {
	BitFlipPacketConn
	writer N.VectorisedPacketWriter
}

func (c *VectorisedBitFlipConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	flipBits(p)
	return bufio.WriteVectorisedPacket(c.writer, [][]byte{p}, M.SocksaddrFromNet(addr))
}

func (c *VectorisedBitFlipConn) WriteVectorisedPacket(buffers []*buf.Buffer, destination M.Socksaddr) error {
	for _, buffer := range buffers {
		data := buffer.Bytes()
		flipBits(data)
	}
	return c.writer.WriteVectorisedPacket(buffers, destination)
}
