package conn

// Logger abstracts application logger
type Logger interface {
	Info(v ...interface{})
	Warning(v ...interface{})
	Warningf(format string, v ...interface{})
}
