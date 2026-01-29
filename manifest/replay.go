package manifest

func Replay(snapshot []byte, logEntries [][]byte) (*Manifest, error) {
	var m *Manifest
	var err error

	if len(snapshot) > 0 {
		m, err = DecodeSnapshot(snapshot)
		if err != nil {
			return nil, err
		}
	}
	if m == nil {
		m = &Manifest{}
	}

	for _, data := range logEntries {
		if len(data) == 0 {
			continue
		}
		entry, err := DecodeLogEntry(data)
		if err != nil {
			return nil, err
		}
		m = ApplyLogEntry(m, entry)
	}

	return m, nil
}
