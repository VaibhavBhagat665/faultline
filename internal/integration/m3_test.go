package integration

import (
	"testing"
)

// TestM3ExactSlice tests the hash over the exact (seed, msgID) combinations used in cluster
func TestM3ExactSlice(t *testing.T) {
	for _, threshold := range []float64{0.08, 0.15} {
		dropped := 0
		total := 0
		
		// Test seeds 1-50, msgIDs 1-250 per seed (matching cluster usage)
		for seed := int64(1); seed <= 50; seed++ {
			for msgID := uint64(1); msgID <= 250; msgID++ {
				total++
				h := uint64(seed) ^ msgID
				h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9
				h = (h ^ (h >> 27)) * 0x94d049bb133111eb
				h = h ^ (h >> 31)
				const mask53 = (1 << 53) - 1
				f := float64(h&mask53) / float64(1<<53)
				if f < threshold {
					dropped++
				}
			}
		}
		
		rate := float64(dropped) / float64(total)
		t.Logf("threshold=%.2f seeds=1-50 msgIDs=1-250 dropped=%d total=%d rate=%.4f",
			threshold, dropped, total, rate)
	}
}
