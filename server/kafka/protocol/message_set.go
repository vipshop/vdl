package protocol

type MessageElement struct {
	Offset int64
	Size   int32
	*Message
}

type MessageSet struct {
	Messages                []*MessageElement
	PartialTrailingMessages bool
}

func (ms *MessageSet) Encode(e PacketEncoder) error {
	//e.PutInt64(ms.Offset)
	//e.Push(&SizeField{})
	//for _, m := range ms.Messages {
	//	if err := m.Encode(e); err != nil {
	//		return err
	//	}
	//}
	//e.Pop()
	return nil
}

func (ms *MessageSet) Decode(d PacketDecoder) error {
	var err error
	for d.remaining() > 0 {
		me := new(MessageElement)
		me.Message = new(Message)
		if me.Offset, err = d.Int64(); err != nil {
			return err
		}
		if me.Size, err = d.Int32(); err != nil {
			return err
		}
		err = me.Message.Decode(d)
		switch err {
		case nil:
			ms.Messages = append(ms.Messages, me)
		case ErrInsufficientData:
			ms.PartialTrailingMessages = true
			return nil
		default:
			return err
		}
	}
	return nil
}
