package protocol

type FetchPartitionResponse struct {
	Partition     int32
	ErrorCode     int16
	HighWatermark int64
	Messages      []*FetchMessage
}

type FetchResponse struct {
	Topic              string
	PartitionResponses []*FetchPartitionResponse
}

type FetchResponses struct {
	ThrottleTimeMs int32
	Responses      []*FetchResponse
	// Temp for msg encode, will not write to client
	APIVersion int16
}

type FetchMessage struct {
	Offset int64
	Record []byte
}

func (r *FetchResponses) FetchResponseSize() int {
	var size int = 0
	if r.APIVersion > 0 {
		size += 4
	}
	for _, respTopic := range r.Responses {
		size += len(respTopic.Topic)
		for _, respPartition := range respTopic.PartitionResponses {
			size += 14
			for _, msg := range respPartition.Messages {
				size += len(msg.Record) + 8
			}
		}
	}
	return size
}

func (r *FetchResponses) Encode(e PacketEncoder) (err error) {
	if r.APIVersion > 0 {
		e.PutInt32(r.ThrottleTimeMs)
	}
	if err = e.PutArrayLength(len(r.Responses)); err != nil {
		return err
	}
	for _, r := range r.Responses {
		if err = e.PutString(r.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(r.PartitionResponses)); err != nil {
			return err
		}
		for _, p := range r.PartitionResponses {
			e.PutInt32(p.Partition)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.HighWatermark)

			//calculate messageSet size
			var messageSetSize int32 = 0
			for _, msg := range p.Messages {
				// offset int64 , messageSize int32
				messageSetSize = messageSetSize + 8 + 4
				//Record
				messageSetSize = messageSetSize + int32(len(msg.Record))
			}
			e.PutInt32(messageSetSize)

			// put msgs
			for _, msg := range p.Messages {
				e.PutInt64(msg.Offset)
				e.PutBytes(msg.Record)
			}

		}
	}
	return nil
}

func (r *FetchResponses) Decode(d PacketDecoder) error {
	//var err error
	////r.ThrottleTimeMs, err = d.Int32()
	////if err != nil {
	////	return err
	////}
	//responseCount, err := d.ArrayLength()
	//r.Responses = make([]*FetchResponse, responseCount)
	//
	//for i := range r.Responses {
	//	resp := &FetchResponse{}
	//	resp.Topic, err = d.String()
	//	if err != nil {
	//		return err
	//	}
	//	partitionCount, err := d.ArrayLength()
	//	ps := make([]*FetchPartitionResponse, partitionCount)
	//	for j := range ps {
	//		p := &FetchPartitionResponse{}
	//		p.Partition, err = d.Int32()
	//		if err != nil {
	//			return err
	//		}
	//		p.ErrorCode, err = d.Int16()
	//		if err != nil {
	//			return err
	//		}
	//		p.HighWatermark, err = d.Int64()
	//		if err != nil {
	//			return err
	//		}
	//		p.RecordSet, err = d.Bytes()
	//		if err != nil {
	//			return err
	//		}
	//		ps[j] = p
	//	}
	//	resp.PartitionResponses = ps
	//	r.Responses[i] = resp
	//}
	panic("should not enter FetchResponses Decode")
	return nil
}
