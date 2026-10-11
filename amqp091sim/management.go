package amqp091sim

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// ManagementHandler serves the part of RabbitMQ's management HTTP API
// (https://www.rabbitmq.com/docs/http-api-reference) that lists what the
// broker holds, from the broker's own state: GET /api/queues/{vhost}
// (name, vhost, durable, messages, consumers) and GET
// /api/exchanges/{vhost}/{exchange}/bindings/source (source, destination,
// destination_type, routing_key). The vhost and exchange are
// percent-encoded path segments ("/" is %2F). Logins are HTTP basic with
// the broker's users; a wrong one is 401 with RabbitMQ's
// {"error":"not_authorized","reason":"Login failed"} body, an unknown
// vhost or exchange 404 {"error":"Object Not Found","reason":"Not Found"}.
func (b *Broker) ManagementHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if pw, known := b.opts.Users[user]; !ok || !known || pw != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="RabbitMQ Management"`)
			mgmtJSON(w, http.StatusUnauthorized, map[string]string{"error": "not_authorized", "reason": "Login failed"})
			return
		}
		if r.Method != http.MethodGet {
			mgmtJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method Not Allowed"})
			return
		}
		segs := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/api/"), "/")
		for i, s := range segs {
			segs[i], _ = url.PathUnescape(s)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		switch {
		case len(segs) == 2 && segs[0] == "queues":
			if !b.vhostLocked(segs[1]) {
				mgmtJSON(w, http.StatusNotFound, map[string]string{"error": "Object Not Found", "reason": "Not Found"})
				return
			}
			out := []map[string]any{}
			for _, q := range b.queues {
				if q.vhost != segs[1] {
					continue
				}
				out = append(out, map[string]any{"name": q.name, "vhost": q.vhost, "durable": true, "auto_delete": false,
					"messages": len(q.ready) + q.unacked, "messages_ready": len(q.ready), "messages_unacknowledged": q.unacked,
					"consumers": len(q.consumers), "state": "running", "type": "classic"})
			}
			sort.Slice(out, func(i, j int) bool { return out[i]["name"].(string) < out[j]["name"].(string) })
			mgmtJSON(w, http.StatusOK, out)
		case len(segs) == 5 && segs[0] == "exchanges" && segs[3] == "bindings" && segs[4] == "source":
			e := b.exchanges[segs[1]+"|"+segs[2]]
			if e == nil {
				mgmtJSON(w, http.StatusNotFound, map[string]string{"error": "Object Not Found", "reason": "Not Found"})
				return
			}
			out := []map[string]any{}
			for _, bd := range e.bindings {
				out = append(out, map[string]any{"source": e.name, "vhost": segs[1], "destination": bd.queue,
					"destination_type": "queue", "routing_key": bd.key, "arguments": map[string]any{}, "properties_key": bd.key})
			}
			mgmtJSON(w, http.StatusOK, out)
		default:
			mgmtJSON(w, http.StatusNotFound, map[string]string{"error": "Object Not Found", "reason": "Not Found"})
		}
	})
}

func (b *Broker) vhostLocked(vh string) bool {
	for _, v := range b.opts.VHosts {
		if v == vh {
			return true
		}
	}
	return false
}

func mgmtJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
