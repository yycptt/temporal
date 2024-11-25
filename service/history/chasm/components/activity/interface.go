package activity

type ScheduleRequest struct {
	Input []byte
}
type ScheduleResponse struct{}

type RecordStartedRequest struct {
	RefToken []byte
}

type RecordStartedResponse struct {
	RefToken []byte
	Input    []byte
}

type RecordCompletedRequest struct {
	RefToken []byte
	Output   []byte
}

type RecordCompletedResponse struct{}
