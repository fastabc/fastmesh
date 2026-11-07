package catalog

import "encoding/json"

// DecodeObject reads the JSON object representation written by the controller.
func DecodeObject(data []byte, out any) error { return json.Unmarshal(data, out) }
