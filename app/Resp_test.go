package main

import "testing"

func TestWriteHelpersIgnoreNilConn(t *testing.T) {
	// none of these may panic when there is no client to reply to
	writeStr(nil, "OK")
	writeErr(nil, "boom")
	writeBulkStr(nil, false, "v")
	writeBulkStr(nil, true, "")
	writeInt(nil, 1)
	writeArray(nil, []string{"a"})
	writeNullArray(nil)
}
