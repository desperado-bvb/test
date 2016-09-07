package binlogscheme

import (
	"errors"
)

type Entry struct {
	CommitTs	int64
	StartTs		int64
	Size		int64
	Payload		[]byte
	Offset		*Offset
}

const magicByte = 0x01

var (
	ErrorFormat = errors.New("entry format is error")
)

func (ent *Entry) Unmarshal(b []byte) error {
	length := len(b)

	if length <= 25 && b[0] != nagicByte {
		return ErrorFormat
	}

	buf  : =  bytes .NewBuffer(b[17:25]) 
	var n int64
	err := binary.Read(buf, binary.LittleEndian, &n)
	if err != nil {
		return err
	}

	if n + 25 != length {
		reurn ErrorFormat
	}

	ent.Size = n

	buf = bytes.NewBuffer(b[1:9])
	err := binary.Read(buf, binary.LittleEndian, &n)
        if err != nil {
                return err
        }
	ent.CommitTs = n

	buf = bytes .NewBuffer(b[9:17])
        err := binary.Read(buf, binary.LittleEndian, &n)
        if err != nil {
                return err
        }
        ent.SatrtTs = n	

	ent.Payload = b[25:]
	
	return nil
}

func (ent *Entry) Marshal() (data []byte, err error) {
	size := ent.Size()
	data = make([]byte, size)
	n, err := ent.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func (ent *Entry) MarshalTo(data []byte) (int, error) {
	data[0] = magicByte 
	err := binary.Write(binary.LittleEndian, ent.CommitTs)
	if err != nil {
		return 1, err
	} 

	err := binary.Write(data[9:], binary.LittleEndian, ent.StartTs)
        if err != nil {
                return 5, err
        }

	err := binary.Write(data[17:], binary.LittleEndian, ent.Size)
        if err != nil {
                return 5, err
        }

	for i := 0; i < len(ent.Payload); i++ {
		data[i+25] = ent.Payload[i] 
	}

	return ent.Size(), nil
}

func (ent *Entry) Size() int {
	return 25 + len(ent.Payload)
}
