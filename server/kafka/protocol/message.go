package protocol

type Message struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

func (m *Message) Encode(e PacketEncoder) error {
	e.PutInt32(m.Crc)
	e.PutInt8(m.MagicByte)
	e.PutInt8(m.Attributes) // attributes
	//MagicByte = 1
	if m.MagicByte == 1 {
		e.PutInt64(m.Timestamp)
	} else {
		if m.MagicByte == 2 {
			return ErrMagicByte
		}
	}
	if err := e.PutBytes(m.Key); err != nil {
		return err
	}
	if err := e.PutBytes(m.Value); err != nil {
		return err
	}
	return nil
}

func (m *Message) Decode(d PacketDecoder) error {
	var err error
	if m.Crc, err = d.Int32(); err != nil {
		return err
	}
	if m.MagicByte, err = d.Int8(); err != nil {
		return err
	}
	//Attributes, be ignored
	if m.Attributes, err = d.Int8(); err != nil {
		return err
	}
	if m.MagicByte == 1 {
		t, err := d.Int64()
		if err != nil {
			return err
		}
		m.Timestamp = t
	} else {
		if m.MagicByte == 2 {
			return ErrMagicByte
		}
	}
	if m.Key, err = d.Bytes(); err != nil {
		return err
	}
	if m.Value, err = d.Bytes(); err != nil {
		return err
	}
	return nil
}
