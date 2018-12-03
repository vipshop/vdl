package protocol

type BrokerV2 struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

type BrokerV1 struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

type BrokerV0 struct {
	NodeID int32
	Host   string
	Port   int32
}

type PartitionMetadata struct {
	PartitionErrorCode int16
	ParititionID       int32
	Leader             int32
	Replicas           []int32
	ISR                []int32
}

//version 2
type TopicMetadataV2 struct {
	TopicErrorCode    int16
	Topic             string
	IsInternal        bool
	PartitionMetadata []*PartitionMetadata
}

//version 1
type TopicMetadataV1 struct {
	TopicErrorCode    int16
	Topic             string
	IsInternal        bool
	PartitionMetadata []*PartitionMetadata
}

//version 0
type TopicMetadataV0 struct {
	TopicErrorCode    int16
	Topic             string
	PartitionMetadata []*PartitionMetadata
}

//version 0
type MetadataResponseV0 struct {
	Brokers       []*BrokerV0
	TopicMetadata []*TopicMetadataV0
}

//version 1
type MetadataResponseV1 struct {
	Brokers       []*BrokerV1
	ControllerID  int32
	TopicMetadata []*TopicMetadataV1
}

//version 2
type MetadataResponseV2 struct {
	Brokers       []*BrokerV2
	ClusterID     string
	ControllerID  int32
	TopicMetadata []*TopicMetadataV2
}

func (r *MetadataResponseV2) Encode(e PacketEncoder) (err error) {
	if err = e.PutArrayLength(len(r.Brokers)); err != nil {
		return err
	}
	for _, b := range r.Brokers {
		e.PutInt32(b.NodeID)
		if err = e.PutString(b.Host); err != nil {
			return err
		}
		e.PutInt32(b.Port)
		e.PutString(b.Rack)
	}

	e.PutString(r.ClusterID)
	e.PutInt32(r.ControllerID)

	if err = e.PutArrayLength(len(r.TopicMetadata)); err != nil {
		return err
	}
	for _, t := range r.TopicMetadata {
		e.PutInt16(t.TopicErrorCode)
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		e.PutBool(t.IsInternal)
		if err = e.PutArrayLength(len(t.PartitionMetadata)); err != nil {
			return err
		}
		for _, p := range t.PartitionMetadata {
			e.PutInt16(p.PartitionErrorCode)
			e.PutInt32(p.ParititionID)
			e.PutInt32(p.Leader)
			if err = e.PutInt32Array(p.Replicas); err != nil {
				return err
			}
			if err = e.PutInt32Array(p.ISR); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *MetadataResponseV1) Encode(e PacketEncoder) (err error) {
	if err = e.PutArrayLength(len(r.Brokers)); err != nil {
		return err
	}
	for _, b := range r.Brokers {
		e.PutInt32(b.NodeID)
		if err = e.PutString(b.Host); err != nil {
			return err
		}
		e.PutInt32(b.Port)
		e.PutString(b.Rack)
	}

	e.PutInt32(r.ControllerID)

	if err = e.PutArrayLength(len(r.TopicMetadata)); err != nil {
		return err
	}
	for _, t := range r.TopicMetadata {
		e.PutInt16(t.TopicErrorCode)
		if err = e.PutString(t.Topic); err != nil {
			return err
		}
		e.PutBool(t.IsInternal)
		if err = e.PutArrayLength(len(t.PartitionMetadata)); err != nil {
			return err
		}
		for _, p := range t.PartitionMetadata {
			e.PutInt16(p.PartitionErrorCode)
			e.PutInt32(p.ParititionID)
			e.PutInt32(p.Leader)
			if err = e.PutInt32Array(p.Replicas); err != nil {
				return err
			}
			if err = e.PutInt32Array(p.ISR); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *MetadataResponseV0) Encode(e PacketEncoder) (err error) {
	if err = e.PutArrayLength(len(r.Brokers)); err != nil {
		return err
	}
	for _, b := range r.Brokers {
		e.PutInt32(b.NodeID)
		if err = e.PutString(b.Host); err != nil {
			return err
		}
		e.PutInt32(b.Port)
	}

	if err = e.PutArrayLength(len(r.TopicMetadata)); err != nil {
		return err
	}
	for _, t := range r.TopicMetadata {
		e.PutInt16(t.TopicErrorCode)
		if err = e.PutString(t.Topic); err != nil {
			return err
		}

		if err = e.PutArrayLength(len(t.PartitionMetadata)); err != nil {
			return err
		}
		for _, p := range t.PartitionMetadata {
			e.PutInt16(p.PartitionErrorCode)
			e.PutInt32(p.ParititionID)
			e.PutInt32(p.Leader)
			if err = e.PutInt32Array(p.Replicas); err != nil {
				return err
			}
			if err = e.PutInt32Array(p.ISR); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *MetadataResponseV0) Decode(d PacketDecoder) error {
	panic("should not enter MetadataResponseV0.Decode")
	return nil
}

func (r *MetadataResponseV1) Decode(d PacketDecoder) error {
	panic("should not enter MetadataResponseV1.Decode")
	return nil
}

func (r *MetadataResponseV2) Decode(d PacketDecoder) error {
	//brokerCount, err := d.ArrayLength()
	//r.Brokers = make([]*Broker, brokerCount)
	//for i := range r.Brokers {
	//	nodeID, err := d.Int32()
	//	if err != nil {
	//		return err
	//	}
	//	host, err := d.String()
	//	if err != nil {
	//		return err
	//	}
	//	port, err := d.Int32()
	//	if err != nil {
	//		return err
	//	}
	//	r.Brokers[i] = &Broker{
	//		NodeID: nodeID,
	//		Host:   host,
	//		Port:   port,
	//	}
	//}
	//topicCount, err := d.ArrayLength()
	//r.TopicMetadata = make([]*TopicMetadata, topicCount)
	//for i := range r.TopicMetadata {
	//	m := &TopicMetadata{}
	//	m.TopicErrorCode, err = d.Int16()
	//	if err != nil {
	//		return err
	//	}
	//	m.Topic, err = d.String()
	//	if err != nil {
	//		return err
	//	}
	//	partitionCount, err := d.ArrayLength()
	//	if err != nil {
	//		return err
	//	}
	//	partitions := make([]*PartitionMetadata, partitionCount)
	//	for i := range partitions {
	//		p := &PartitionMetadata{}
	//		p.PartitionErrorCode, err = d.Int16()
	//		if err != nil {
	//			return err
	//		}
	//		p.ParititionID, err = d.Int32()
	//		if err != nil {
	//			return err
	//		}
	//		p.Leader, err = d.Int32()
	//		if err != nil {
	//			return err
	//		}
	//		p.Replicas, err = d.Int32Array()
	//		if err != nil {
	//			return err
	//		}
	//		p.ISR, err = d.Int32Array()
	//		partitions[i] = p
	//	}
	//	m.PartitionMetadata = partitions
	//	r.TopicMetadata[i] = m
	//}
	panic("should not enter MetadataResponse.Decode")
	return nil
}
