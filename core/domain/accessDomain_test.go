package domain

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTopicName(t *testing.T) {
	valid := []string{"orders", "orders.v2", "my-topic_1", "with space", "ÆØÅ", "a..b", strings.Repeat("a", MaxTopicNameLength)}
	for _, name := range valid {
		if err := ValidateTopicName(TopicName(name)); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v, want nil", name, err)
		}
	}
	invalid := []string{
		"", ".", "..", "../escaped", ".hidden", "a/b", "/absolute", "nested/../../escaped", `a\b`, `..\escaped`,
		"a\x00b", "line\nbreak", "tab\there", "del\x7f", strings.Repeat("a", MaxTopicNameLength+1),
	}
	for _, name := range invalid {
		if err := ValidateTopicName(TopicName(name)); !errors.Is(err, ErrInvalidTopicName) {
			t.Errorf("ValidateTopicName(%q) = %v, want ErrInvalidTopicName", name, err)
		}
	}
}
