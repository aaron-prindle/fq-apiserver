package fq

import (
	"math"
)

// TODO(aaron-prindle) currently testing with one concurrent request
const C = 1 // const C = 300

// TODO(aaron-prindle) currently service time "G" is not implemented entirely
const G = 100000 //   100000 nanoseconds = 100 milliseconds || const G = 60000000 nanoseconds = 1 minute

type Packet struct {
	// request   http.Request
	item interface{}
	// virfinish float64
	size int
	// key       uint64
	queueidx  int
	seq       int
	starttime float64
}

type Queue struct {
	Packets           []*Packet
	virstart          float64
	RequestsExecuting int
	// RequestsExecuting []*Packet
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

func initQueues(n int, key uint64) []*Queue {
	queues := []*Queue{}
	for i := 0; i < n; i++ {
		queues = append(queues, &Queue{
			Packets: []*Packet{},
		})
	}
	return queues
}

func (p *Packet) virfinish(J int, q *FQScheduler) float64 {
	// The virtual finish time of request number J in the queue
	// (counting from J=1 for the head) is J * G + (virtual start time).

	J++ // counting from J=1 for the head (eg: queue.Packets[0] -> J=1)
	// if J*G overflows
	//    throw an error?
	jg := float64(J * G)
	if math.IsInf(jg, 1) {
		panic("float overflowed")
	}
	return jg + q.queues[p.queueidx].virstart

}

func (p *Packet) finishRequest(q *FQScheduler) {
	q.synctime()
	S := q.nowAsUnixNano() - p.starttime

	// When a request finishes being served, and the actual service time was S,
	// the queue’s virtual start time is decremented by G - S.
	q.queues[p.queueidx].virstart -= G - S

	// remove from requests executing
	q.queues[p.queueidx].RequestsExecuting--
}
