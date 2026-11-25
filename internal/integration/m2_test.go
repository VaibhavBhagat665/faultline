package integration

import (
	"testing"
)

func TestM2HashDistribution(t *testing.T) {
	for _, threshold := range []float64{0.08, 0.15} {
		for _, seed := range []int64{1, 42, 999} {
			dropped := 0
			histogram := make([]int, 16)

			for msgID := uint64(0); msgID < 1000000; msgID++ {
				// splitmix64 mixer
				h := uint64(seed) ^ msgID
				h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
				h = (h ^ (h >> 27)) * 0x94d049bb133111eb
				h = h ^ (h >> 31)

				// Convert to float64 in [0,1) using top 53 bits
				const mask53 = (1 << 53) - 1
				f := float64(h&mask53) / float64(1<<53)

				if f < threshold {
					dropped++
				}

				// Histogram
				bucket := int(f * 16)
				if bucket >= 16 {
					bucket = 15
				}
				histogram[bucket]++
			}

			rate := float64(dropped) / 1000000.0
			t.Logf("threshold=%.2f seed=%d rate=%.6f", threshold, seed, rate)

			// Check rate
			if rate < threshold-0.005 || rate > threshold+0.005 {
				t.Errorf("threshold=%.2f seed=%d rate=%.6f outside [%.6f, %.6f]",
					threshold, seed, rate, threshold-0.005, threshold+0.005)
			}
		}

		// Print histogram for first seed
		t.Logf("Histogram for threshold=%.2f seed=1:", threshold)
		seed := int64(1)
		histogram2 := make([]int, 16)
		for msgID := uint64(0); msgID < 1000000; msgID++ {
			h := uint64(seed) ^ msgID
			h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
			h = (h ^ (h >> 27)) * 0x94d049bb133111eb
			h = h ^ (h >> 31)
			const mask53 = (1 << 53) - 1
			f := float64(h&mask53) / float64(1<<53)
			bucket := int(f * 16)
			if bucket >= 16 {
				bucket = 15
			}
			histogram2[bucket]++
		}
		for i, count := range histogram2 {
			t.Logf("  bucket %2d: %d", i, count)
		}
	}
}
