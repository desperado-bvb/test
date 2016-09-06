package binlogscheme

import "errors"

type Entry struct {
	CommitTs	int64 `json:"commitTs"`
	StartTs		int64 `json:"startTs"`
	Size		int64 `json:"size"`
	Payload		[]byte`json:"payload"`
}

var (
	ErrorFormat = errors.New("entry format is error")
)

func (ent *Entry) Unmarshal(b []byte) error {
	length := len(b)

	if length <= 25 && b[0] != 0x01 {
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

	buf = bytes .NewBuffer(b[1:9])
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
