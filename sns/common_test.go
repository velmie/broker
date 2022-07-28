package sns

import "testing"

type isFIFOTest struct {
	in  string
	out bool
}

var isFIFOTests = []isFIFOTest{
	{
		in:  "",
		out: false,
	},
	{
		in:  ".fifo",
		out: false,
	},
	{
		in:  "abc.fifo",
		out: true,
	},
	{
		in:  "abc.fifo.abc",
		out: false,
	},
}

func TestIsFifo(t *testing.T) {
	for _, tt := range isFIFOTests {
		if isFifo(tt.in) != tt.out {
			t.Errorf("isFifo(%q): expected %t, got %t", tt.in, tt.out, !tt.out)
		}
	}
}
