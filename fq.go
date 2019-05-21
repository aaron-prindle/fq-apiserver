package fq

import (
	"math"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

type FQScheduler struct {
	lock   sync.Mutex
	queues []*Queue
	clock  clock.Clock
	// Verify float64 has enough bits
	// We will want to be careful about having enough bits. For example, in float64, 1e20 + 1e0 == 1e20.
	vt           float64
	C            float64
	G            float64
	lastRealTime time.Time
	robinidx     int
}

// TODO(aaron-prindle) add concurrency enforcement - 'C'
func (q *FQScheduler) chooseQueue(packet *Packet) *Queue {
	if packet.queueidx < 0 || packet.queueidx > len(q.queues) {
		panic("no matching queue for packet")
	}
	return q.queues[packet.queueidx]
}

func NewFQScheduler(queues []*Queue, clock clock.Clock) *FQScheduler {
	fq := &FQScheduler{
		queues: queues,
		clock:  clock,
		vt:     0,
	}
	return fq
}

// TODO(aaron-prindle) verify that the time units are correct/matching
func (q *FQScheduler) nowAsUnixNano() float64 {
	return float64(q.clock.Now().UnixNano())
}

func (q *FQScheduler) Enqueue(packet *Packet) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.synctime()

	queue := q.chooseQueue(packet)
	queue.enqueue(packet)
	q.updateTime(packet, queue)
}

func (q *FQScheduler) getVirtualTime() float64 {
	return q.vt
}

func (q *FQScheduler) synctime() {
	realNow := q.clock.Now()
	timesincelast := realNow.Sub(q.lastRealTime).Nanoseconds()
	q.lastRealTime = realNow
	q.vt += float64(timesincelast) * q.getvirtualtimeratio()
}

func (q *FQScheduler) getvirtualtimeratio() float64 {
	NEQ := 0
	reqs := 0
	for _, queue := range q.queues {
		reqs += queue.RequestsExecuting
		// It might be best to delete this line. If everything is working
		//  correctly, there will be no waiting packets if reqs < C on current
		//  line 85; if something is going wrong, it is more accurate to say
		// that virtual time advanced due to the requests actually executing.

		// reqs += len(queue.Packets)
		if len(queue.Packets) > 0 || queue.RequestsExecuting > 0 {
			NEQ++
		}
	}
	// no active flows, virtual time does not advance (also avoid div by 0)
	if NEQ == 0 {
		return 0
	}
	return min(float64(reqs), float64(C)) / float64(NEQ)
}

func (q *FQScheduler) updateTime(packet *Packet, queue *Queue) {
	// When a request arrives to an empty queue with no requests executing
	// len(queue.Packets) == 1 as enqueue has just happened prior (vs  == 0)
	if len(queue.Packets) == 1 && queue.RequestsExecuting == 0 {
		// the queue’s virtual start time is set to getVirtualTime().
		queue.virstart = q.getVirtualTime()
	}
}

func (q *FQScheduler) FinishPacketAndDeque(p *Packet) (*Packet, bool) {
	q.FinishPacket(p)
	return q.Dequeue()
}

func (q *FQScheduler) FinishPacket(p *Packet) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.synctime()
	S := q.clock.Since(p.startTime).Nanoseconds()

	// When a request finishes being served, and the actual service time was S,
	// the queue’s virtual start time is decremented by G - S.
	q.queues[p.queueidx].virstart -= G - float64(S)

	// request has finished, remove from requests executing
	q.queues[p.queueidx].RequestsExecuting--
}

func (q *FQScheduler) Dequeue() (*Packet, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	// TODO(aaron-prindle) unsure if this should be here...
	// q.synctime()
	queue := q.selectQueue()

	if queue == nil {
		return nil, false
	}
	packet, ok := queue.dequeue()

	if ok {
		// When a request is dequeued for service -> q.virstart += G
		queue.virstart += G

		packet.startTime = q.clock.Now()
		// request dequeued, service has started
		queue.RequestsExecuting++
	}
	return packet, ok
}

func (q *FQScheduler) roundrobinqueue() int {
	q.robinidx = (q.robinidx + 1) % len(q.queues)
	return q.robinidx
}

func (q *FQScheduler) selectQueue() *Queue {
	minvirfinish := math.Inf(1)
	var minqueue *Queue
	var minidx int
	for range q.queues {
		idx := q.roundrobinqueue()
		queue := q.queues[idx]
		if len(queue.Packets) != 0 {
			curvirfinish := queue.VirtualFinish(0)
			if curvirfinish < minvirfinish {
				minvirfinish = curvirfinish
				minqueue = queue
				minidx = q.robinidx
			}
		}
	}
	q.robinidx = minidx
	return minqueue
}
