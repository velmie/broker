package natsjs

import (
	"fmt"
	"slices"
)

func validateFilterSubjects(failures *configErrors, subjects []string) {
	if len(subjects) == 0 {
		failures.add("subjects", ruleRequired, "subjects must contain at least one NATS filter")
		return
	}
	seen := make(map[string]struct{}, len(subjects))
	for i, subject := range subjects {
		field := fmt.Sprintf("subjects[%d]", i)
		if !validConfigSubject(subject, true) {
			failures.add(field, ruleType, field+" must be a valid NATS subject filter")
		}
		if _, ok := seen[subject]; ok {
			failures.add(field, ruleUnique, field+" must be unique")
		}
		seen[subject] = struct{}{}
	}
}

func nativeFilterSubjects(subject string, subjects []string) []string {
	filters := slices.Clone(subjects)
	if subject != "" {
		filters = append(filters, subject)
	}
	return filters
}

func equalFilterSubjects(expected []string, subject string, subjects []string) bool {
	want := slices.Clone(expected)
	actual := nativeFilterSubjects(subject, subjects)
	slices.Sort(want)
	slices.Sort(actual)
	return slices.Equal(want, actual)
}
