package client

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type OrderedMap struct {
	Keys   []string
	Values map[string]interface{}
}

func (m *OrderedMap) ensureInit() {
	if m.Values == nil {
		m.Values = make(map[string]interface{})
	}
}

func (m *OrderedMap) Get(key string) (interface{}, bool) {
	if m == nil || m.Values == nil {
		return nil, false
	}
	v, ok := m.Values[key]
	return v, ok
}

func (m *OrderedMap) Set(key string, value interface{}) {
	m.ensureInit()
	if _, exists := m.Values[key]; !exists {
		m.Keys = append(m.Keys, key)
	}
	m.Values[key] = value
}

func (m *OrderedMap) UnmarshalJSON(data []byte) error {
	m.Keys = nil
	m.Values = nil
	m.ensureInit()

	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return fmt.Errorf("expected JSON object")
	}

	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return err
		}
		key, ok := kt.(string)
		if !ok {
			return fmt.Errorf("expected string key")
		}

		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}

		var v interface{}
		if err := dec.Decode(&v); err != nil {
			return err
		}
		m.Set(key, v)
	}

	endTok, err := dec.Token()
	if err != nil {
		return err
	}
	endDelim, ok := endTok.(json.Delim)
	if !ok || endDelim != '}' {
		return fmt.Errorf("expected end of object")
	}

	return nil
}

func (m OrderedMap) MarshalJSON() ([]byte, error) {
	if m.Values == nil {
		return []byte("{}"), nil
	}

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range m.Keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := json.Marshal(m.Values[k])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
