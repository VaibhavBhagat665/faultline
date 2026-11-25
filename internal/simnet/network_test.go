// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package simnet

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/VaibhavBhagat665/faultline/internal/sim"
)

func TestNetwork_BasicDelivery(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := NetworkConfig{
		Mode:           DeliveryTCP,
		DefaultLatency: 100,
		Jitter:         0,
		RNG:            rng,
	}

	net := NewNetwork(cfg)

	// Send a message at time 0
	payload := []byte("hello")
	msgID := net.Send(1, 2, payload, 0)

	if msgID == 0 {
		t.Fatal("Send should return non-zero message ID")
	}

	if net.InFlight() != 1 {
		t.Errorf("expected 1 in-flight message, got %d", net.InFlight())
	}

	// Poll at time 50 (before delivery)
	ready := net.Poll(50)
	if len(ready) != 0 {
		t.Errorf("no messages should be ready at time 50, got %d", len(ready))
	}

	// Poll at time 100 (exact delivery time)
	ready = net.Poll(100)
	if len(ready) != 1 {
		t.Fatalf("expected 1 message at time 100, got %d", len(ready))
	}

	msg := ready[0]
	if msg.From != 1 || msg.To != 2 {
		t.Errorf("expected from=1, to=2, got from=%d, to=%d", msg.From, msg.To)
	}

	if !bytes.Equal(msg.Payload, payload) {
		t.Errorf("payload mismatch: expected %q, got %q", payload, msg.Payload)
	}

	if msg.SendTime != 0 {
		t.Errorf("expected sendTime=0, got %d", msg.SendTime)
	}

	if msg.DeliverTime != 100 {
		t.Errorf("expected deliverTime=100, got %d", msg.DeliverTime)
	}

	// Message should be removed from in-flight
	if net.InFlight() != 0 {
		t.Errorf("expected 0 in-flight after delivery, got %d", net.InFlight())
	}
}

func TestNetwork_Jitter(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 43))
	cfg := NetworkConfig{
		Mode:           DeliveryTCP,
		DefaultLatency: 100,
		Jitter:         50,
		RNG:            rng,
	}

	net := NewNetwork(cfg)

	// Send multiple messages
	const n = 100
	for i := 0; i < n; i++ {
		net.Send(1, 2, []byte("msg"), 0)
	}

	// Collect delivery times
	var deliveryTimes []sim.Time
	for t := sim.Time(0); t <= 200; t++ {
		msgs := net.Poll(t)
		for range msgs {
			deliveryTimes = append(deliveryTimes, t)
		}
	}

	if len(deliveryTimes) != n {
		t.Fatalf("expected %d deliveries, got %d", n, len(deliveryTimes))
	}

	// Check that delivery times are spread out (due to jitter)
	// All should be in [100, 150)
	for i, dt := range deliveryTimes {
		if dt < 100 || dt >= 150 {
			t.Errorf("delivery %d at time %d, expected [100, 150)", i, dt)
		}
	}

	// Check that they're not all the same (jitter should vary)
	allSame := true
	first := deliveryTimes[0]
	for _, dt := range deliveryTimes {
		if dt != first {
			allSame = false
			break
		}
	}

	if allSame {
		t.Error("all delivery times are identical; jitter not working")
	}
}

func TestNetwork_Partition(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := NetworkConfig{
		Mode:           DeliveryTCP,
		DefaultLatency: 100,
		RNG:            rng,
	}

	net := NewNetwork(cfg)

	// Create partition 1 -> 2
	net.Partition(1, 2)

	// Send should return 0 (dropped)
	msgID := net.Send(1, 2, []byte("blocked"), 0)
	if msgID != 0 {
		t.Error("message should be dropped by partition")
	}

	if net.InFlight() != 0 {
		t.Error("partitioned message should not be in-flight")
	}

	// Reverse direction should work
	msgID = net.Send(2, 1, []byte("ok"), 0)
	if msgID == 0 {
		t.Error("reverse direction should not be partitioned")
	}

	if net.InFlight() != 1 {
		t.Error("reverse message should be in-flight")
	}

	// Heal partition
	net.Heal(1, 2)

	// Now 1 -> 2 should work
	msgID = net.Send(1, 2, []byte("healed"), 0)
	if msgID == 0 {
		t.Error("message should be sent after healing")
	}
}

func TestNetwork_DropRate(t *testing.T) {
	rng := rand.New(rand.NewPCG(99, 100))
	cfg := NetworkConfig{
		Mode:           DeliveryUDP,
		DefaultLatency: 100,
		RNG:            rng,
	}

	net := NewNetwork(cfg)
	net.SetDropRate(0.5) // 50% drop rate

	// Send many messages
	const n = 1000
	sent := 0
	for i := 0; i < n; i++ {
		msgID := net.Send(1, 2, []byte("msg"), 0)
		if msgID != 0 {
			sent++
		}
	}

	// Should have dropped approximately 50%
	dropRate := 1.0 - float64(sent)/float64(n)
	if dropRate < 0.4 || dropRate > 0.6 {
		t.Errorf("drop rate should be ~0.5, got %.2f (%d/%d sent)", dropRate, sent, n)
	}

	t.Logf("Sent %d/%d messages (%.1f%% dropped)", sent, n, dropRate*100)
}

func TestNetwork_DuplicateRate(t *testing.T) {
	rng := rand.New(rand.NewPCG(77, 88))
	cfg := NetworkConfig{
		Mode:           DeliveryUDP,
		DefaultLatency: 100,
		RNG:            rng,
	}

	net := NewNetwork(cfg)
	net.SetDuplicateRate(0.3) // 30% duplicate rate

	// Send messages
	const n = 100
	for i := 0; i < n; i++ {
		net.Send(1, 2, []byte("msg"), 0)
	}

	// Poll and count deliveries
	var delivered int
	for t := sim.Time(0); t <= 300; t++ {
		msgs := net.Poll(t)
		delivered += len(msgs)
	}

	// Should have more than n due to duplicates
	if delivered <= n {
		t.Errorf("expected duplicates, but delivered=%d, sent=%d", delivered, n)
	}

	dupRate := (float64(delivered) - float64(n)) / float64(n)
	t.Logf("Delivered %d messages from %d sent (%.1f%% duplicates)", delivered, n, dupRate*100)

	// Should be roughly 30% duplicates
	if dupRate < 0.2 || dupRate > 0.4 {
		t.Errorf("duplicate rate should be ~0.3, got %.2f", dupRate)
	}
}

func TestNetwork_ReorderWindow(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 22))
	cfg := NetworkConfig{
		Mode:           DeliveryUDP,
		DefaultLatency: 100,
		Jitter:         0,
		RNG:            rng,
	}

	net := NewNetwork(cfg)
	net.SetReorderWindow(50) // Messages can be reordered within 50 time units

	// Send messages in sequence
	const n = 10
	for i := 0; i < n; i++ {
		net.Send(1, 2, []byte{byte(i)}, 0)
	}

	// Collect delivery order
	var order []byte
	for t := sim.Time(0); t <= 200; t++ {
		msgs := net.Poll(t)
		for _, msg := range msgs {
			order = append(order, msg.Payload[0])
		}
	}

	if len(order) != n {
		t.Fatalf("expected %d deliveries, got %d", n, len(order))
	}

	// Check if any reordering occurred
	inOrder := true
	for i := 0; i < n; i++ {
		if order[i] != byte(i) {
			inOrder = false
			break
		}
	}

	if inOrder {
		t.Error("expected reordering with ReorderWindow=50, but messages arrived in order")
	}

	t.Logf("Message order: %v (expected some reordering)", order)
}

func TestNetwork_ClearFaults(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := NetworkConfig{
		Mode:           DeliveryUDP,
		DefaultLatency: 100,
		RNG:            rng,
	}

	net := NewNetwork(cfg)

	// Set all faults
	net.Partition(1, 2)
	net.SetDropRate(0.5)
	net.SetDuplicateRate(0.3)
	net.SetReorderWindow(50)

	// Clear all
	net.ClearFaults()

	// Check that partition is cleared
	if net.IsPartitioned(1, 2) {
		t.Error("partition should be cleared")
	}

	// Check that drop rate is cleared
	if net.faults.DropRate != 0 {
		t.Error("drop rate should be 0")
	}

	// Send should work without drops
	const n = 100
	sent := 0
	for i := 0; i < n; i++ {
		if msgID := net.Send(1, 2, []byte("msg"), 0); msgID != 0 {
			sent++
		}
	}

	if sent != n {
		t.Errorf("all messages should be sent after clear, got %d/%d", sent, n)
	}
}

func TestNetwork_DropAllInFlight(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	cfg := NetworkConfig{
		Mode:           DeliveryTCP,
		DefaultLatency: 1000,
		RNG:            rng,
	}

	net := NewNetwork(cfg)

	// Send many messages
	for i := 0; i < 100; i++ {
		net.Send(1, 2, []byte("msg"), 0)
	}

	if net.InFlight() != 100 {
		t.Fatalf("expected 100 in-flight, got %d", net.InFlight())
	}

	// Drop all
	net.DropAllInFlight()

	if net.InFlight() != 0 {
		t.Errorf("expected 0 in-flight after drop, got %d", net.InFlight())
	}

	// Poll should return nothing
	msgs := net.Poll(2000)
	if len(msgs) != 0 {
		t.Errorf("expected no messages after drop, got %d", len(msgs))
	}
}
