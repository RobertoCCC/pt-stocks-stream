package poller

import (
	"context"

	"github.com/RobertoCCC/pt-stocks-stream/internal/redisbus"
)

// RedisWriter adapts a redis pub/sub channel to io.Writer so json.Encoder
// can drive it unchanged. Each Write becomes one PUBLISH; this matches how
// json.Encoder.Encode emits exactly one frame per call (object + newline).
//
// Exposed at package level because both cmd/poller (split deploy) and
// cmd/allinone (free-tier collocated deploy) need to publish ticks the
// same way.
type RedisWriter struct {
	Bus     *redisbus.Bus
	Channel string
}

// Write publishes the payload as-is. The newline appended by json.Encoder
// is harmless on the wire and helps when humans inspect the channel with
// `redis-cli SUBSCRIBE`.
func (w *RedisWriter) Write(p []byte) (int, error) {
	// Use Background here: the json.Encoder doesn't carry our run context,
	// and a per-tick publish should respect the network's own timeout via
	// go-redis defaults rather than be cut short by a stale deadline.
	if err := w.Bus.Publish(context.Background(), w.Channel, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
