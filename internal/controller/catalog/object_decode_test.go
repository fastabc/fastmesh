package catalog

import "testing"

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
