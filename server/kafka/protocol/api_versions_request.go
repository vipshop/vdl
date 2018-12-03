package protocol

type ApiVersionsRequest struct {
}

func (r *ApiVersionsRequest) Key() int16 {
	return APIVersionsKey
}

func (r *ApiVersionsRequest) Version() int16 {
	return 1
}
