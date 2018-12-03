package protocol

import "github.com/vipshop/vdl/pkg/glog"

type PartitionData struct {
	Partition      int32
	MessageSetSize int32
	MessageSet
}

type TopicData struct {
	Topic string
	Data  []*PartitionData
}

type ProduceRequest struct {
	Acks      int16
	Timeout   int32
	TopicData []*TopicData
}

func (r *ProduceRequest) Encode(e PacketEncoder) (err error) {
	e.PutInt16(r.Acks)
	e.PutInt32(r.Timeout)
	if err = e.PutArrayLength(len(r.TopicData)); err != nil {
		return err
	}
	for _, td := range r.TopicData {
		if err = e.PutString(td.Topic); err != nil {
			return err
		}
		if err = e.PutArrayLength(len(td.Data)); err != nil {
			return err
		}
		for _, d := range td.Data {
			e.PutInt32(d.Partition)
			e.PutInt32(d.MessageSetSize)
			err = d.MessageSet.Encode(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ProduceRequest) Print() {
	glog.Infof("ProduceRequest=%v", r)
}

//version 2
//解析一个生产消息的请求，赋值到r中
func (r *ProduceRequest) Decode(d PacketDecoder) (err error) {
	r.Acks, err = d.Int16()
	if err != nil {
		return err
	}
	r.Timeout, err = d.Int32()
	if err != nil {
		return err
	}
	//数组长度，数组都是以长度+内容的方式组成
	topicCount, err := d.ArrayLength()
	r.TopicData = make([]*TopicData, topicCount)
	for i := range r.TopicData {
		td := new(TopicData)
		r.TopicData[i] = td
		td.Topic, err = d.String()
		if err != nil {
			return err
		}
		dataCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		//数据集数组长度
		td.Data = make([]*PartitionData, dataCount)
		for j := range td.Data {
			data := new(PartitionData)
			td.Data[j] = data
			data.Partition, err = d.Int32()
			if err != nil {
				return err
			}
			data.MessageSetSize, err = d.Int32()
			if err != nil {
				return err
			}
			err = data.MessageSet.Decode(d)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ProduceRequest) Key() int16 {
	return ProduceKey
}

func (r *ProduceRequest) Version() int16 {
	return 2
}
