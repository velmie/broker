package broker

type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	AlreadySubscribed = Error("already subscribed")
)
