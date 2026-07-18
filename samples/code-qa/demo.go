package demo

import "fmt"

// Add returns the sum of a and b.
func Add(a, b int) int {
	return a + b
}

func TestAdd(t *testing.T) {
	if Add(1, 2) != 3 {
		t.Fatal(fmt.Sprintf("bad: %d", Add(1, 2)))
	}
}
