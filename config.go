package logdb

// Config struct for configure DB
type Config struct {
	SplitSize       int
	WriteBufferSize int
}

var (
	// DefaultConfig is default config for DB
	DefaultConfig = &Config{
		SplitSize:       1024 * 1024 * 8,
		WriteBufferSize: 1024 * 256,
	}
)
