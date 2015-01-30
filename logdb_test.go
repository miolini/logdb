package logdb

import "testing"
import "crypto/rand"

func TestWrite(t *testing.T) {
	cfg := Config{}
	cfg.WriteBufferSize = 1024 * 256
	cfg.SplitSize = 1024 * 1024 * 64
	db, err := Open("testdb/", &cfg)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	defer db.Close()
	buf := []byte("test")
	for i:=0;i<1024*1024*32;i++ {
		rand.Read(buf[2:])
		db.Write(buf)
	}
}
