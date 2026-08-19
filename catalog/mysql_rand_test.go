package catalog

import "testing"

func TestMySQLRandFromSeed(t *testing.T) {
	cases := []struct {
		seed int64
		want float64
	}{
		{1, 0.6046602879796196},
		{2, 0.16729663442585624},
		{3, 0.7199826688373036},
		{100, 0.8165026937796166},
	}
	for _, tc := range cases {
		got := mysqlRandFromSeed(tc.seed)
		if got != tc.want {
			t.Fatalf("mysqlRandFromSeed(%d) = %v; want %v", tc.seed, got, tc.want)
		}
	}
}
