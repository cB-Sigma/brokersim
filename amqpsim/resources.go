package amqpsim

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ResourcesOptions say who may list a namespace's entities.
type ResourcesOptions struct {
	// Keys are shared access policies: key name to key. A SAS token
	// (SharedAccessSignature sr=…&sig=…&se=…&skn=…) is checked with the
	// key as text, as Service Bus and Event Hubs sign (IoT Hub decodes
	// its keys from base64 instead).
	Keys map[string]string
	// Bearer accepts a Microsoft Entra access token; nil accepts none.
	Bearer func(token string) bool
}

// ResourcesHandler serves the Service Bus and Event Hubs entity listing
// over HTTPS as Atom feeds, from the broker's own queues and event hubs:
// GET /$Resources/Queues, /$Resources/Topics, /{topic}/Subscriptions and
// /$Resources/EventHubs, paged with $skip and $top (default 100). A queue
// named topic/Subscriptions/sub is a topic's subscription. Each entry's
// title is the entity's name and its content a QueueDescription,
// TopicDescription, SubscriptionDescription or EventHubDescription
// (MessageCount, PartitionCount). A missing, expired or wrongly signed
// token is 401 with the namespace's XML <Error> body. From the Service
// Bus REST reference (https://learn.microsoft.com/rest/api/servicebus/)
// and its SAS authentication guide.
func (b *Broker) ResourcesHandler(o ResourcesOptions) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !o.authorised(r.Header.Get("Authorization")) {
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `<Error><Code>401</Code><Detail>InvalidSignature: The token has an invalid signature.</Detail></Error>`)
			return
		}
		path := strings.Trim(r.URL.Path, "/")
		skip, _ := strconv.Atoi(r.URL.Query().Get("$skip"))
		top, err := strconv.Atoi(r.URL.Query().Get("$top"))
		if err != nil || top <= 0 {
			top = 100
		}
		type ent struct {
			name, kind string
			body       string
		}
		var all []ent
		b.mu.Lock()
		switch {
		case path == "$Resources/Queues":
			for _, q := range b.queues {
				if !strings.Contains(q.name, "/Subscriptions/") {
					all = append(all, ent{q.name, "QueueDescription", fmt.Sprintf("<MessageCount>%d</MessageCount>", len(q.ready))})
				}
			}
		case path == "$Resources/Topics":
			seen := map[string]bool{}
			for _, q := range b.queues {
				if t, _, ok := strings.Cut(q.name, "/Subscriptions/"); ok && !seen[t] {
					seen[t] = true
					all = append(all, ent{t, "TopicDescription", "<SubscriptionCount>1</SubscriptionCount>"})
				}
			}
		case strings.HasSuffix(path, "/Subscriptions") && !strings.HasPrefix(path, "$"):
			topic := strings.TrimSuffix(path, "/Subscriptions")
			for _, q := range b.queues {
				if t, sub, ok := strings.Cut(q.name, "/Subscriptions/"); ok && t == topic {
					all = append(all, ent{sub, "SubscriptionDescription", fmt.Sprintf("<MessageCount>%d</MessageCount>", len(q.ready))})
				}
			}
		case path == "$Resources/EventHubs":
			for _, h := range b.hubs {
				ids := make([]string, len(h.partitions))
				for i := range ids {
					ids[i] = "<string>" + strconv.Itoa(i) + "</string>"
				}
				all = append(all, ent{h.name, "EventHubDescription",
					fmt.Sprintf("<PartitionCount>%d</PartitionCount><PartitionIds>%s</PartitionIds>", len(h.partitions), strings.Join(ids, ""))})
			}
		default:
			b.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml; charset=utf-8")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<Error><Code>404</Code><Detail>The requested resource does not exist.</Detail></Error>`)
			return
		}
		b.mu.Unlock()
		sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })
		if skip > len(all) {
			skip = len(all)
		}
		page := all[skip:]
		if len(page) > top {
			page = page[:top]
		}
		var sb strings.Builder
		base := "https://" + r.Host + "/" + path
		sb.WriteString(`<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://www.w3.org/2005/Atom"><title type="text">` + xmlText(path) + `</title><id>` + xmlText(base) + `</id>`)
		for _, e := range page {
			sb.WriteString(`<entry><id>` + xmlText(base+"/"+e.name) + `</id><title type="text">` + xmlText(e.name) + `</title><updated>` +
				time.Now().UTC().Format(time.RFC3339) + `</updated><content type="application/xml"><` + e.kind +
				` xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">` +
				e.body + `<Status>Active</Status></` + e.kind + `></content></entry>`)
		}
		sb.WriteString(`</feed>`)
		w.Header().Set("Content-Type", "application/atom+xml;type=feed;charset=utf-8")
		fmt.Fprint(w, sb.String())
	})
}

func xmlText(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func (o ResourcesOptions) authorised(h string) bool {
	if tok, ok := strings.CutPrefix(h, "Bearer "); ok {
		return o.Bearer != nil && o.Bearer(tok)
	}
	tok, ok := strings.CutPrefix(h, "SharedAccessSignature ")
	if !ok {
		return false
	}
	q, err := url.ParseQuery(tok)
	if err != nil {
		return false
	}
	key, known := o.Keys[q.Get("skn")]
	exp, err := strconv.ParseInt(q.Get("se"), 10, 64)
	if !known || err != nil || time.Now().Unix() > exp {
		return false
	}
	mac := hmac.New(sha256.New, []byte(key))
	rawSR := ""
	for _, part := range strings.Split(tok, "&") {
		if v, ok := strings.CutPrefix(part, "sr="); ok {
			rawSR = v // signed as sent, still percent-encoded
		}
	}
	mac.Write([]byte(rawSR + "\n" + q.Get("se")))
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(want), []byte(q.Get("sig")))
}
