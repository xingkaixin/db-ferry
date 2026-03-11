package utils

import "testing"

func TestProgressManagerWithTotal(t *testing.T) {
	pm := NewProgressManagerWithUnit(2, "test", "")
	if pm.bar == nil {
		t.Fatal("NewProgressManagerWithUnit() should initialize progress bar")
	}

	pm.Increment()
	pm.Increment()
	pm.Increment()
	if pm.current != 2 {
		t.Fatalf("current = %d, want 2", pm.current)
	}

	pm.SetCurrent(10)
	if pm.current != 2 {
		t.Fatalf("SetCurrent should clamp to total, got %d", pm.current)
	}

	pm.Finish()
	pm.Finish()
}

func TestProgressManagerWithoutTotal(t *testing.T) {
	pm := NewProgressManager(-1, "unknown")
	pm.Increment()
	pm.Increment()
	if pm.current != 2 {
		t.Fatalf("current = %d, want 2", pm.current)
	}

	pm.SetCurrent(10)
	if pm.current != 10 {
		t.Fatalf("SetCurrent on unknown total should keep value, got %d", pm.current)
	}
	pm.Finish()
}
