package ehscylla

import (
	"encoding/json"

	eh "github.com/looplab/eventhorizon"
)

type Encoder interface {
	Marshal(interface{}) ([]byte, error)
	UnmarshalEventData(eh.EventType, []byte) (eh.EventData, error)
	UnmarshalEventMetaData([]byte) (map[string]interface{}, error)
	String() string
}

type jsonEncoder struct{}

func (jsonEncoder) Marshal(data interface{}) ([]byte, error) {
	if data != nil {
		return json.Marshal(data)
	}
	return nil, nil
}

func (jsonEncoder) UnmarshalEventData(eventType eh.EventType, raw []byte) (data eh.EventData, err error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if data, err = eh.CreateEventData(eventType); err == nil {
		if err = json.Unmarshal(raw, data); err == nil {
			return data, nil
		}
	}
	return nil, err
}

func (jsonEncoder) UnmarshalEventMetaData(raw []byte) (metadata map[string]interface{}, err error) {
	if len(raw) == 0 {
		return nil, nil
	}

	if err := json.Unmarshal(raw, &metadata); err != nil {
		return nil, err

	}
	return metadata, nil
}

func (jsonEncoder) String() string {
	return "json"
}
