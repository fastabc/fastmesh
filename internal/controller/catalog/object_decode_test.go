package catalog

import (
	"testing"

	"github.com/fastabc/fastmesh/model"
)

func TestDecodeObjectUsesJSONTypes(t *testing.T) {
	for _, body := range []string{`{"value":"ok"}`, `{"value":42}`, "value: ok", "{"} {
		var out struct {
			Value string `json:"value"`
		}
		err := DecodeObject([]byte(body), &out)
		if body == `{"value":"ok"}` {
			if err != nil || out.Value != "ok" {
				t.Fatalf("%+v %v", out, err)
			}
		} else if err == nil {
			t.Fatalf("accepted non-JSON or coerced type: %s", body)
		}
	}
}

// These seeds formerly belonged to a removed YAML differential fuzz target.
// Keep the relevant JSON behavior executable instead of retaining inert corpus.
func TestDecodeObjectFormerFuzzCases(t *testing.T) {
	var out struct {
		Endpoint model.Endpoint `json:"endpoint"`
	}
	if err := DecodeObject([]byte(`{"endpoint":{"tenAnt":0}}`), &out); err == nil {
		t.Fatal("numeric tenant coerced into a string")
	}
	if err := DecodeObject([]byte("{\"\":{\"\":\"\xbe\"}}"), &out); err != nil {
		t.Fatalf("ignored unknown field rejected: %v", err)
	}
}
