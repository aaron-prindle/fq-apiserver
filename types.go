package fq

import "time"

// TODO(aaron-prindle) currently testing with one concurrent request
const DEFAULT_C = 1 // const C = 300

const DEFAULT_G = 1 //   1 second (virtual time is in seconds)

// Packet is a temporary container for "requests" with additional tracking fields
// required for the functionality FQScheduler as well as testing
type Packet struct {
	item        interface{}
	servicetime float64
	queueidx    int
	seq         int
	startTime   time.Time
}

// Queue is an array of packets with additional metadata required for
// the FQScheduler
type Queue struct {
	Packets           []*Packet
	virstart          float64
	RequestsExecuting int
}

// Enqueue enqueues a packet into the queue
func (q *Queue) Enqueue(packet *Packet) {
	q.Packets = append(q.Packets, packet)
}

// Dequeue dequeues a packet from the queue
func (q *Queue) Dequeue() (*Packet, bool) {
	if len(q.Packets) == 0 {
		return nil, false
	}
	packet := q.Packets[0]
	q.Packets = q.Packets[1:]

	return packet, true
}

// InitQueues is a convenience method for initializing an array of n queues
func InitQueues(n int) []*Queue {
	queues := make([]*Queue, 0, n)
	for i := 0; i < n; i++ {
		queues = append(queues, &Queue{
			Packets: []*Packet{},
		})
	}
	return queues
}

// VirtualFinish returns the expected virtual finish time of the packet at
// index J in the queue with estimated finish time G
func (q *Queue) VirtualFinish(J int, G float64) float64 {
	// The virtual finish time of request number J in the queue
	// (counting from J=1 for the head) is J * G + (virtual start time).

	// counting from J=1 for the head (eg: queue.Packets[0] -> J=1) - J+1
	jg := float64(J+1) * float64(G)
	return jg + q.virstart
}
