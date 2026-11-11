package natsjs_test

import (
	"reflect"
	"testing"

	"github.com/velmie/broker/natsjs/v4"
)

func TestDurableFilterConfigurationSchema(t *testing.T) {
	schema := reflect.TypeFor[natsjs.DurableConsumerConfig]()
	field, ok := schema.FieldByName("Subjects")
	if !ok || field.Tag.Get("k") != "subjects" || field.Tag.Get("validate") != "required,min=1,unique,dive,required" ||
		field.Tag.Get("hint") == "" {
		t.Fatalf("subjects metadata: %+v", field)
	}
	if _, present := field.Tag.Lookup("default"); present {
		t.Fatal("subjects has an implicit default")
	}
	if _, present := schema.FieldByName("Subject"); present {
		t.Fatal("singular selector remains")
	}
}
