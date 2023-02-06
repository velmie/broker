package natsjs

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	ErrSubjectInvalid = Error("invalid subscription subject")
	ErrServiceName    = Error("ServiceName cannot be empty")
	ErrStreamName     = Error("StreamName cannot be empty")
	ErrSubjectPrefix  = Error("SubjectPrefix cannot be empty")
)
