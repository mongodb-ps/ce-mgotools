package record

import "testing"

func TestBinaryOrder(t *testing.T) {
	for name, a := range map[string][]string{
		"COMPONENTS":           COMPONENTS,
		"OPERATIONS":           OPERATIONS,
		"OPERATORS_COMPARISON": OPERATORS_COMPARISON,
		"OPERATORS_EXPRESSION": OPERATORS_EXPRESSION,
		"OPERATORS_LOGICAL":    OPERATORS_LOGICAL,
		"SEVERITIES":           SEVERITIES} {
		if index := testSortOrder(a); index > -1 {
			t.Errorf("%s not sorted; %s (%d) out of order", name, a[index], index)
		}
	}
}

func testSortOrder(a []string) int {
	for i := 1; i < len(a); i += 1 {
		if a[i-1] > a[i] {
			return i
		}
	}

	return -1
}
