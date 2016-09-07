package binlog

import (
	"bufio"
	"io"
)

type decoder struct {
	brs	[]*bufio.Reader
	offset *Binlog.BinlogOffset
}

func newDecoder(offset *Binlog.BinlogOffset, r ...io.Reader) {
	readers := make([]*bufio.Reader, len(r))
	for i := range  r {
		readers[i] = bufio.NewReader(r[i])
	}


	return &decoder {
		brs:	readers,
		offset:	offset,
	}
}

func (d *decoder) decode(ent *binlogscheme.Entry) error {
	if len(d.brs) == 0 {
		return io.EOF
	}

	l, err := readInt64(d.brs[0])
	if err == io.EOF || (err == nil && l == 0) {
		d.brs = d.brs[1:]
		if len(d.brs) == 0 {
			return io.EOF
		}
		d.offset.Index += 1
		d.offset.Offset = 0
		return d.decode(ent)
	}
	if err != nil {
		return err
	}

	entBytes, padBytes := decodeFrameSize(l)

	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	if err := ent.Unmarshal(data[:entBytes], d.offset); err != nil {
		return err
	}

	d.offset.Offset += entBytes +  padBytes + 8

	return nil
}

func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56))
	if lenField < 0 {
		padBytes = int64((uint64(lenField) >> 56) & 0x7)
	}
	return
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
