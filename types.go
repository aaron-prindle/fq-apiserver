package fq

import "time"

// TODO(aaron-prindle) currently testing with one concurrent request
const C = 1 // const C = 300

const G = 100000 //   100000 nanoseconds = .1 milliseconds || const G = 60000000000 nanoseconds = 1 minute

type Packet struct {
	item      interface{}
	size      int
	queueidx  int
	seq       int
	startTime time.Time
}

type Queue struct {
	Packets           []*Packet
	virstart          float64
	RequestsExecuting int
}

func (q *Queue) enqueue(packet *Packet) {
	q.Packets = append(q.Packets, packet)
}

func (q *Queue) dequeue() (*Packet, bool) {
	if len(q.Packets) == 0 {
		return nil, false
	}
	packet := q.Packets[0]
	q.Packets = q.Packets[1:]
	return packet, true
}

func initQueues(n int) []*Queue {
	queues := make([]*Queue, 0, n)
	for i := 0; i < n; i++ {
		queues = append(queues, &Queue{
			Packets: []*Packet{},
		})
	}
	return queues
}

func (q *Queue) VirtualFinish(J int) float64 {
	// The virtual finish time of request number J in the queue
	// (counting from J=1 for the head) is J * G + (virtual start time).

	J++ // counting from J=1 for the head (eg: queue.Packets[0] -> J=1)
	jg := float64(J) * float64(G)
	return jg + q.virstart
}
