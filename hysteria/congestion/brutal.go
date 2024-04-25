package congestion

import (
	"time"

	"github.com/sagernet/quic-go/congestion"
	"github.com/sagernet/quic-go/monotime"
	"github.com/sagernet/sing/common/logger"
)

const (
	pktInfoSlotCount           = 5 // slot index is based on seconds, so this is basically how many seconds we sample
	minSampleCount             = 50
	minAckRate                 = 0.8
	congestionWindowMultiplier = 2
)

var _ congestion.CongestionControlEx = &BrutalSender{}

type BrutalSender struct {
	rttStats        congestion.RTTStatsProvider
	bps             congestion.ByteCount
	maxDatagramSize congestion.ByteCount
	pacer           *pacer

	pktInfoSlots [pktInfoSlotCount]pktInfo
	ackRate      float64
}

type pktInfo struct {
	Timestamp int64
	AckCount  uint64
	LossCount uint64
}

func NewBrutalSender(bps uint64, initialMaxDatagramSize congestion.ByteCount, _ bool, _ logger.Logger) *BrutalSender {
	bs := &BrutalSender{
		bps:             congestion.ByteCount(bps),
		maxDatagramSize: initialMaxDatagramSize,
		ackRate:         1,
	}
	bs.pacer = newPacer(initialMaxDatagramSize, func() congestion.ByteCount {
		return congestion.ByteCount(float64(bs.bps) / bs.ackRate)
	})
	return bs
}

func (b *BrutalSender) SetRTTStatsProvider(rttStats congestion.RTTStatsProvider) {
	b.rttStats = rttStats
}

func (b *BrutalSender) TimeUntilSend(bytesInFlight congestion.ByteCount) monotime.Time {
	return b.pacer.TimeUntilSend()
}

func (b *BrutalSender) HasPacingBudget(now monotime.Time) bool {
	return b.pacer.Budget(now) >= b.maxDatagramSize
}

func (b *BrutalSender) CanSend(bytesInFlight congestion.ByteCount) bool {
	return bytesInFlight <= b.GetCongestionWindow()
}

func (b *BrutalSender) GetCongestionWindow() congestion.ByteCount {
	rtt := b.rttStats.SmoothedRTT()
	if rtt <= 0 {
		return 10240
	}
	cwnd := congestion.ByteCount(float64(b.bps) * rtt.Seconds() * congestionWindowMultiplier / b.ackRate)
	if cwnd < b.maxDatagramSize {
		cwnd = b.maxDatagramSize
	}
	return cwnd
}

func (b *BrutalSender) OnPacketSent(sentTime monotime.Time, bytesInFlight congestion.ByteCount,
	packetNumber congestion.PacketNumber, bytes congestion.ByteCount, isRetransmittable bool,
) {
	b.pacer.SentPacket(sentTime, bytes)
}

func (b *BrutalSender) OnPacketAcked(number congestion.PacketNumber, ackedBytes congestion.ByteCount,
	priorInFlight congestion.ByteCount, eventTime monotime.Time,
) {
	// Stub
}

func (b *BrutalSender) OnCongestionEvent(number congestion.PacketNumber, lostBytes congestion.ByteCount,
	priorInFlight congestion.ByteCount,
) {
	// Stub
}

func (b *BrutalSender) OnCongestionEventEx(priorInFlight congestion.ByteCount, eventTime monotime.Time, ackedPackets []congestion.AckedPacketInfo, lostPackets []congestion.LostPacketInfo) {
	currentTimestamp := int64(time.Duration(eventTime) / time.Second)
	slot := currentTimestamp % pktInfoSlotCount
	if b.pktInfoSlots[slot].Timestamp == currentTimestamp {
		b.pktInfoSlots[slot].LossCount += uint64(len(lostPackets))
		b.pktInfoSlots[slot].AckCount += uint64(len(ackedPackets))
	} else {
		// uninitialized slot or too old, reset
		b.pktInfoSlots[slot].Timestamp = currentTimestamp
		b.pktInfoSlots[slot].AckCount = uint64(len(ackedPackets))
		b.pktInfoSlots[slot].LossCount = uint64(len(lostPackets))
	}
	b.updateAckRate(currentTimestamp)
}

func (b *BrutalSender) OnAppLimited(bytesInFlight congestion.ByteCount) {
}

func (b *BrutalSender) OnPacketNeutered(packetNumber congestion.PacketNumber) {
}

func (b *BrutalSender) OnPacketsLost(leastUnacked congestion.PacketNumber) {
}

func (b *BrutalSender) SetMaxDatagramSize(size congestion.ByteCount) {
	b.maxDatagramSize = size
	b.pacer.SetMaxDatagramSize(size)
}

func (b *BrutalSender) updateAckRate(currentTimestamp int64) {
	minTimestamp := currentTimestamp - pktInfoSlotCount
	var ackCount, lossCount uint64
	for _, info := range b.pktInfoSlots {
		if info.Timestamp < minTimestamp {
			continue
		}
		ackCount += info.AckCount
		lossCount += info.LossCount
	}
	if ackCount+lossCount < minSampleCount {
		b.ackRate = 1
		return
	}
	rate := float64(ackCount) / float64(ackCount+lossCount)
	if rate < minAckRate {
		b.ackRate = minAckRate
		return
	}
	b.ackRate = rate
}

func (b *BrutalSender) InSlowStart() bool {
	return false
}

func (b *BrutalSender) InRecovery() bool {
	return false
}

func (b *BrutalSender) MaybeExitSlowStart() {}

func (b *BrutalSender) OnRetransmissionTimeout(packetsRetransmitted bool) {}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
