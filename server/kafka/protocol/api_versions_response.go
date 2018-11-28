package protocol

type ApiVersion struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type APIVersionsResponse struct {
	ErrorCode      int16
	ApiVersions    []*ApiVersion
	ThrottleTimeMs int32

	// Save Request Version, use in Encode, which decide Encode func use ThrottleTimeMs or not
	RequestVersion int16
}

func (r *APIVersionsResponse) Encode(e PacketEncoder) error {
	e.PutInt16(r.ErrorCode)
	e.PutArrayLength(len(r.ApiVersions))
	for _, v := range r.ApiVersions {
		e.PutInt16(v.ApiKey)
		e.PutInt16(v.MinVersion)
		e.PutInt16(v.MaxVersion)
	}
	if r.RequestVersion == 1 {
		e.PutInt32(r.ThrottleTimeMs)
	}
	return nil
}

func (r *APIVersionsResponse) Decode(d PacketDecoder) error {
	//var err error
	//r.ErrorCode, err = d.Int16()
	//if err != nil {
	//	return err
	//}
	//l, err := d.ArrayLength()
	//if err != nil {
	//	return err
	//}
	//r.ApiVersions = make([]*ApiVersion, l)
	//for i := range r.ApiVersions {
	//	apiVersion := new(ApiVersion)
	//	apiVersion.ApiKey, err = d.Int16()
	//	if err != nil {
	//		return err
	//	}
	//	apiVersion.MinVersion, err = d.Int16()
	//	if err != nil {
	//		return err
	//	}
	//	apiVersion.MaxVersion, err = d.Int16()
	//	if err != nil {
	//		return err
	//	}
	//	r.ApiVersions[i] = apiVersion
	//}
	//r.ThrottleTimeMs, err = d.Int32()
	//if err != nil {
	//	return err
	//}
	panic("should not enter APIVersionsResponse.Decode")
	return nil
}
