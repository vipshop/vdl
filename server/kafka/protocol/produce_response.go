package protocol

type ProducePartitionResponse struct {
	Partition      int32
	ErrorCode      int16
	BaseOffset     int64
	Timestamp      int64
	OffsetChannels []<-chan interface{} //just for vdl pipeline
}

type ProduceResponse struct {
	Topic              string
	PartitionResponses []*ProducePartitionResponse
}

type ProduceResponses struct {
	Responses      []*ProduceResponse
	ThrottleTimeMs int32
	version        int16
}

func (rs *ProduceResponses) Encode(e PacketEncoder) error {
	e.PutArrayLength(len(rs.Responses))
	for _, r := range rs.Responses {
		e.PutString(r.Topic)
		e.PutArrayLength(len(r.PartitionResponses))
		for _, p := range r.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.BaseOffset)
			if rs.Version() == 2 {
				e.PutInt64(p.Timestamp)
			}
		}
	}
	if rs.Version() == 1 || rs.Version() == 2 {
		e.PutInt32(rs.ThrottleTimeMs)
	}

	return nil
}

func (r *ProduceResponses) Decode(d PacketDecoder) error {
	var err error
	l, err := d.ArrayLength()
	if err != nil {
		return err
	}
	r.Responses = make([]*ProduceResponse, l)
	for i := range r.Responses {
		resp := new(ProduceResponse)
		r.Responses[i] = resp
		resp.Topic, err = d.String()
		if err != nil {
			return err
		}
		pl, err := d.ArrayLength()
		if err != nil {
			return err
		}

		ps := make([]*ProducePartitionResponse, pl)
		for j := range ps {
			p := new(ProducePartitionResponse)
			ps[j] = p
			p.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			p.ErrorCode, err = d.Int16()
			if err != nil {
				return err
			}
			p.BaseOffset, err = d.Int64()
			if err != nil {
				return err
			}
			if r.Version() == 2 {
				p.Timestamp, err = d.Int64()
				if err != nil {
					return err
				}
			}
		}
		resp.PartitionResponses = ps
	}
	if r.Version() == 1 || r.Version() == 2 {
		r.ThrottleTimeMs, err = d.Int32()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ProduceResponses) Version() int16 {
	return r.version
}

func (r *ProduceResponses) SetVersion(v int16) {
	r.version = v
}
