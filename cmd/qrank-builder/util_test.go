package main

import (
	"fmt"
	"testing"
)

func TestFormatLine(t *testing.T) {
	tests := []struct{ site, title, value, expected string }{
		{"als.wiki", "Wa\u0308he", "Q2595950", "als.wiki/wähe Q2595950"},
		{"az.wiki", "Bakı", "Q9248", "az.wiki/bakı Q9248"},
		{"az.wiki", "BAKI", "Q9248", "az.wiki/bakı Q9248"},
		{"azx.wiki", "BAKI", "Q9248", "azx.wiki/baki Q9248"},
		{"de.wiki", "BAKI", "Q9248", "de.wiki/baki Q9248"},
		{"tr.wiki", "Diyarbakır", "Q83387", "tr.wiki/diyarbakır Q83387"},
		{"tr.wiki", "DİYARBAKIR", "Q83387", "tr.wiki/diyarbakır Q83387"},
		{"de.wiki", "Straße", "Q34442", "de.wiki/strasse Q34442"},
		{"xx.wiki", "Space C", "U+0020", "xx.wiki/space_c U+0020"},
		{"xx.wiki", "Tab\tC", "U+0007", "xx.wiki/tab_c U+0007"},
		{"xx.wiki", "Zero\x00" + "C", "U+0000", "xx.wiki/zero_c U+0000"},
	}
	for _, c := range tests {
		if got := formatLine(c.site, c.title, c.value); c.expected != got {
			msg := fmt.Sprintf("expected %q, got %q", c.expected, got)
			t.Error(msg)
		}
	}
}
