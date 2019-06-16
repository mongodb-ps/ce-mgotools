package sorter

import "testing"

func TestKey_Less(t *testing.T) {
	k := Key{
		"b",  // 0
		"c",  // 1
		"a",  // 2
		"_",  // 3
		"_a", // 4
		"_a", // 5
		"_b", // 6
		"_",  // 7
		"__", // 8
		"",   // 9
		"$",  // 10
		"$a", // 11
	}

	expect := map[int]int{
		0: 1,
		3: 2,
		4: 6,
		7: 8,
		9: 2,
	}
	for i, j := range expect {
		if !k.Less(i, j) {
			t.Errorf("'%s' (%d) less than '%s' (%d) = false, should be true", k[i], i, k[j], j)
		}
	}

	fail := map[int]int{
		3: 2,
		1: 9,
		2: 3,
	}
	for i, j := range fail {
		if k.Less(i, j) {
			t.Errorf("'%s' (%d) less than '%s' (%d) = true, should be false", k[i], i, k[j], j)
		}
	}
}

func TestKey_Swap(t *testing.T) {
	k := Key{"a", "b", "c"}
	k.Swap(0, 1)

	if k[0] != "b" && k[1] != "a" {
		t.Error("swap failed to swap")
	}
}
