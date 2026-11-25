// Copyright 2026 Vaibhav Bhagat
// SPDX-License-Identifier: Apache-2.0

package sim

import "time"

// Time represents virtual time in nanoseconds since the simulation start.
// Virtual time is monotonically increasing and has no relationship to wall-clock time.
// All time in the simulation comes from the virtual clock.
type Time int64

// Duration returns the time as a time.Duration.
func (t Time) Duration() time.Duration {
	return time.Duration(t)
}

// Add returns t + d.
func (t Time) Add(d time.Duration) Time {
	return t + Time(d)
}

// Sub returns t - other.
func (t Time) Sub(other Time) time.Duration {
	return time.Duration(t - other)
}

// Before returns true if t < other.
func (t Time) Before(other Time) bool {
	return t < other
}

// After returns true if t > other.
func (t Time) After(other Time) bool {
	return t > other
}

// String returns a human-readable representation of the time.
func (t Time) String() string {
	return time.Duration(t).String()
}

// Clock is a virtual clock that returns the current simulation time.
type Clock struct {
	now Time
}

// NewClock creates a new virtual clock starting at time 0.
func NewClock() *Clock {
	return &Clock{now: 0}
}

// Now returns the current virtual time.
func (c *Clock) Now() Time {
	return c.now
}

// Advance advances the clock to the given time.
// The new time must not be earlier than the current time.
// Panics if t < c.now (time cannot go backward).
func (c *Clock) Advance(t Time) {
	if t < c.now {
		panic("clock: time cannot go backward")
	}
	c.now = t
}
