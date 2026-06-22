package walrusds

import (
	"bytes"
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestMultipartBodyLengthMatchesWriter guards the invariant that the precomputed
// Content-Length (multipartBodyLength) equals the bytes writeMultipartBody
// actually streams, across Go versions of mime/multipart.
func TestMultipartBodyLengthMatchesWriter(t *testing.T) {
	cases := [][]QuiltPart{
		{{Identifier: "0", Data: []byte("hello")}},
		{
			{Identifier: "0", Data: bytes.Repeat([]byte("a"), 1000)},
			{Identifier: "1", Data: []byte{}},
			{Identifier: "42", Data: bytes.Repeat([]byte{0xff}, 65536)},
		},
		{
			{Identifier: "0", Data: []byte{}},
			{Identifier: "665", Data: []byte("z")},
		},
	}

	const boundary = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	for i, parts := range cases {
		var buf bytes.Buffer
		if err := writeMultipartBody(&buf, parts, boundary); err != nil {
			t.Fatalf("case %d: writeMultipartBody: %v", i, err)
		}
		got := int64(buf.Len())
		want := multipartBodyLength(parts, boundary)
		if got != want {
			t.Fatalf("case %d: wrote %d bytes, multipartBodyLength reported %d", i, got, want)
		}
	}
}

// TestStoreQuiltStreamsAndParses exercises the streaming StoreQuilt end to end:
// the server verifies the body is well-formed multipart with a Content-Length
// matching the bytes received, and the client parses the patch IDs back.
func TestStoreQuiltStreamsAndParses(t *testing.T) {
	parts := []QuiltPart{
		{Identifier: "0", Data: []byte("block-zero")},
		{Identifier: "1", Data: bytes.Repeat([]byte("x"), 4096)},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body: %v", err)
		}
		if r.ContentLength != int64(len(body)) {
			t.Errorf("Content-Length=%d but received %d bytes", r.ContentLength, len(body))
		}
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/form-data" {
			t.Errorf("unexpected Content-Type %q (%v)", r.Header.Get("Content-Type"), err)
		}
		mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		sizes := map[string]int{}
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("next part: %v", err)
			}
			data, _ := io.ReadAll(p)
			sizes[p.FormName()] = len(data)
		}
		if sizes["0"] != len("block-zero") || sizes["1"] != 4096 {
			t.Errorf("part sizes mismatch: %v", sizes)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"blobStoreResult":{"newlyCreated":{"blobObject":{"blobId":"QUILTBLOB","size":1,"storage":{"startEpoch":1,"endEpoch":53}}}},"storedQuiltBlobs":[{"identifier":"0","quiltPatchId":"P0"},{"identifier":"1","quiltPatchId":"P1"}]}`)
	}))
	defer srv.Close()

	c := NewClient(ClientConfig{
		PublisherURLs:  []string{srv.URL},
		AggregatorURLs: []string{srv.URL},
	})

	res, err := c.StoreQuilt(context.Background(), parts, 53, false)
	if err != nil {
		t.Fatalf("StoreQuilt: %v", err)
	}
	if res.QuiltID != "QUILTBLOB" {
		t.Errorf("QuiltID = %q, want QUILTBLOB", res.QuiltID)
	}
	if res.EndEpoch != 53 {
		t.Errorf("EndEpoch = %d, want 53", res.EndEpoch)
	}
	if len(res.Patches) != 2 {
		t.Fatalf("got %d patches, want 2", len(res.Patches))
	}
}
