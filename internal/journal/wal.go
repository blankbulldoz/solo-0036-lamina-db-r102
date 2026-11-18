package journal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const MaxFrame = 16 << 20

type WAL struct {
	Base          uint64
	dir           string
	file          *os.File
	lock          *os.File
	Records       []Record
	RecoveredTail int64
	Poisoned      bool
}

func Open(dir string, base uint64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "lamina.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already open: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "journal.lmw"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	w := &WAL{dir: dir, file: f, lock: lock, Base: base}
	info, err := f.Stat()
	if err != nil {
		w.Close()
		return nil, err
	}
	if info.Size() == 0 {
		header := make([]byte, 16)
		copy(header[:4], "LMW1")
		binary.BigEndian.PutUint64(header[4:12], base)
		binary.BigEndian.PutUint32(header[12:], crc32.ChecksumIEEE(header[:12]))
		if _, err = f.Write(header); err == nil {
			err = f.Sync()
		}
		if err != nil {
			w.Close()
			return nil, err
		}
	}
	directory, syncErr := os.Open(dir)
	if syncErr != nil {
		w.Close()
		return nil, syncErr
	}
	syncErr = directory.Sync()
	directory.Close()
	if syncErr != nil {
		w.Close()
		return nil, syncErr
	}
	if _, err = f.Seek(0, 0); err != nil {
		w.Close()
		return nil, err
	}
	if err = w.recover(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}
func (w *WAL) recover() error {
	var header [16]byte
	if _, err := io.ReadFull(w.file, header[:]); err != nil {
		return fmt.Errorf("journal header: %w", err)
	}
	if string(header[:4]) != "LMW1" || crc32.ChecksumIEEE(header[:12]) != binary.BigEndian.Uint32(header[12:]) {
		return fmt.Errorf("invalid journal header")
	}
	actualBase := binary.BigEndian.Uint64(header[4:12])
	if actualBase > w.Base {
		return fmt.Errorf("journal requires newer checkpoint")
	}
	w.Base = actualBase
	offset := int64(16)
	last := w.Base
	for {
		var head [8]byte
		n, err := io.ReadFull(w.file, head[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			w.RecoveredTail = int64(n)
			break
		}
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(head[:4])
		if length == 0 || length > MaxFrame {
			return fmt.Errorf("invalid journal length at %d", offset)
		}
		payload := make([]byte, length)
		n, err = io.ReadFull(w.file, payload)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			w.RecoveredTail = int64(8 + n)
			break
		}
		if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(head[4:]) {
			return fmt.Errorf("journal checksum mismatch at %d", offset)
		}
		var r Record
		if err = json.Unmarshal(payload, &r); err != nil {
			return fmt.Errorf("journal json at %d: %w", offset, err)
		}
		if (r.Format != 1 && r.Format != CurrentFormat) || r.Sequence != last+1 {
			return fmt.Errorf("journal sequence or format mismatch at %d", offset)
		}
		w.Records = append(w.Records, r)
		last = r.Sequence
		offset += int64(8 + length)
	}
	if w.RecoveredTail > 0 {
		if err := w.file.Truncate(offset); err != nil {
			return err
		}
		if err := w.file.Sync(); err != nil {
			return err
		}
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return err
}
func (w *WAL) Append(r Record) error {
	if w.Poisoned {
		return fmt.Errorf("journal is unavailable; restart required")
	}
	if r.Sequence != w.Base+uint64(len(w.Records))+1 {
		return fmt.Errorf("noncontiguous journal sequence")
	}
	r.Format = CurrentFormat
	payload, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(payload) > MaxFrame {
		return fmt.Errorf("commit exceeds journal frame limit")
	}
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[8:], payload)
	if _, err = w.file.Write(frame); err != nil {
		w.Poisoned = true
		return err
	}
	if err = w.file.Sync(); err != nil {
		w.Poisoned = true
		return err
	}
	w.Records = append(w.Records, r)
	return nil
}
func (w *WAL) Close() error {
	err := w.file.Close()
	w.lock.Close()
	return err
}

// Rewrite atomically replaces the journal with the same sequences and base in
// the current format. Records must already be converted. The replacement is
// only installed after every frame has been encoded and synced, so an
// interrupted upgrade leaves the original journal readable by both versions.
func (w *WAL) Rewrite() error {
	if w.Poisoned {
		return fmt.Errorf("journal is unavailable; restart required")
	}
	temp, err := os.CreateTemp(w.dir, ".lamina-journal-")
	if err != nil {
		return err
	}
	path := temp.Name()
	defer os.Remove(path)
	if err = temp.Chmod(0600); err != nil {
		temp.Close()
		return err
	}
	header := make([]byte, 16)
	copy(header[:4], "LMW1")
	binary.BigEndian.PutUint64(header[4:12], w.Base)
	binary.BigEndian.PutUint32(header[12:], crc32.ChecksumIEEE(header[:12]))
	if _, err = temp.Write(header); err != nil {
		temp.Close()
		return err
	}
	for i := range w.Records {
		r := w.Records[i]
		if r.Legacy() {
			temp.Close()
			return fmt.Errorf("journal sequence %d is not converted", r.Sequence)
		}
		r.Format = CurrentFormat
		payload, err := json.Marshal(r)
		if err != nil {
			temp.Close()
			return err
		}
		if len(payload) > MaxFrame {
			temp.Close()
			return fmt.Errorf("journal frame exceeds limit during upgrade")
		}
		frame := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
		copy(frame[8:], payload)
		if _, err = temp.Write(frame); err != nil {
			temp.Close()
			return err
		}
		w.Records[i] = r
	}
	if err = temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = os.Rename(path, filepath.Join(w.dir, "journal.lmw")); err != nil {
		return err
	}
	directory, err := os.Open(w.dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	directory.Close()
	if syncErr != nil {
		return syncErr
	}
	if err = w.file.Close(); err != nil {
		return err
	}
	w.file, err = os.OpenFile(filepath.Join(w.dir, "journal.lmw"), os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	_, err = w.file.Seek(0, io.SeekEnd)
	return err
}
