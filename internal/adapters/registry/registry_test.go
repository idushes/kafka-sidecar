package registry_test

import (
	"encoding/json"
	"fmt"
	"kafka-sidecar/internal/adapters/registry"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

var testTable = []struct {
	MessageBefore []byte
	MessageAfter  []byte
}{
	{
		[]byte(`{"id": "test", "text": "test test test", "archived": false}`),
		[]byte(`{"id": "test", "text": "test test test", "archived": false}`),
	},
	{
		[]byte(`{"id": "test", "text": "test test test", "archived": false, "_updated_at": 1257894000000000000}`),
		[]byte(`{"id": "test", "text": "test test test", "archived": false}`),
	},
}

func TestRegistry(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.URL)
		fmt.Fprintln(w, `{"subject":"test-value","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"test\",\"namespace\":\"xr.test.kafka.avro\",\"doc\":\"\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"text\",\"type\":[\"null\",\"string\"]},{\"name\":\"archived\",\"type\":\"boolean\",\"default\":false}]}"}`)
	}))
	defer ts.Close()

	tr := registry.New(ts.URL, 10)

	for i, msg := range testTable {
		t.Run(fmt.Sprintf("test #%d", i), func(t *testing.T) {
			e, err := tr.Encode("test", msg.MessageBefore)
			require.NoError(t, err)

			b, err := tr.Decode("test", e)
			require.NoError(t, err)

			var m1, m2 map[string]interface{}
			err = json.Unmarshal(msg.MessageAfter, &m1)
			require.NoError(t, err)
			err = json.Unmarshal(b, &m2)
			require.NoError(t, err)

			require.Equal(t, m1, m2)
		})
	}
}
