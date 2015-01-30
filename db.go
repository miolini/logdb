package logdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"compress/gzip"
	"os"
	"path"
	"sync"
)

// DB is struct represent database
type DB struct {
	dir            string
	currentFile    *os.File
	currentRawSize int
	bufWriter      *bufio.Writer
	writer         *gzip.Writer
	mutex          sync.Mutex
	config         *Config
	fileIndex      int
}

// Open will open database at dir location with config
func Open(dir string, config *Config) (db *DB, err error) {
	db = new(DB)
	db.dir = path.Dir(dir + "/")
	if err = os.MkdirAll(db.dir, 0775); err != nil {
		return
	}
	if config == nil {
		config = DefaultConfig
	}
	db.config = config
	err = db.rotate()
	return
}

// Write will write binary entry
func (db *DB) Write(entry []byte) (err error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if db.currentRawSize+len(entry) > db.config.SplitSize {
		db.rotate()
	}
	len := uint32(len(entry))
	err = binary.Write(db.writer, binary.LittleEndian, len)
	if err == nil {
		_, err = db.writer.Write(entry)
	}
	if err == nil {
		db.currentRawSize += 4 + int(len)
	}
	return
}

func (db *DB) rotate() (err error) {
	if db.currentFile != nil {
		if err = db.writer.Close(); err != nil {
			return
		}
		if err = db.syncUnsafe(); err != nil {
			return
		}
		if err = db.currentFile.Close(); err != nil {
			return
		}
		db.fileIndex++
	}
	filename := fmt.Sprintf("%s/%012d.db.gz", db.dir, db.fileIndex)
	db.currentFile, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		return
	}
	db.bufWriter = bufio.NewWriterSize(db.currentFile, db.config.WriteBufferSize)
	db.writer, err = gzip.NewWriterLevel(db.bufWriter, 4)
	db.currentRawSize = 0
	return
}

// Sync syncing data with storage
func (db *DB) Sync() (err error) {
	db.mutex.Lock()
	err = db.syncUnsafe()
	db.mutex.Unlock()
	return
}

func (db *DB) syncUnsafe() (err error) {
	err = db.bufWriter.Flush()
	if err == nil {
		err = db.currentFile.Sync()
	}
	return
}

func (db *DB) Close() {
	db.writer.Close()
	db.bufWriter.Flush()
	db.currentFile.Close()
}