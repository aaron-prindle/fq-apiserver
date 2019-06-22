package fq

import (
	"math"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// FQScheduler is a fair queuing implementation designed for the kube-apiserver.
// FQ is designed for
// 1) dispatching requests to be served rather than packets to be transmitted
// 2) serving multiple requests at once
// 3) accounting for unknown and varying service time
type FQScheduler struct {
	lock         sync.Mutex
	queues       []*Queue
	clock        clock.Clock
	vt           float64
	C            int
	G            float64
	lastRealTime time.Time
	robinidx     int
}

func (q *FQScheduler) chooseQueue(packet *Packet) *Queue {
	if packet.queueidx < 0 || packet.queueidx > len(q.queues) {
		panic("no matching queue for packet")
	}
	return q.queues[packet.queueidx]
}

func NewFQScheduler(queues []*Queue, clock clock.Clock) *FQScheduler {
	fq := &FQScheduler{
		queues:       queues,
		clock:        clock,
		vt:           0,
		lastRealTime: clock.Now(),
		C:            DEFAULT_C,
		G:            DEFAULT_G,
	}
	return fq
}

// Enqueue enqueues a packet into the fair queuing scheduler
func (q *FQScheduler) Enqueue(packet *Packet) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.synctime()

	queue := q.chooseQueue(packet)
	queue.Enqueue(packet)
	q.updateTime(packet, queue)
}

func (q *FQScheduler) getVirtualTime() float64 {
	return q.vt
}

func (q *FQScheduler) synctime() {
	realNow := q.clock.Now()
	timesincelast := realNow.Sub(q.lastRealTime).Seconds()
	q.lastRealTime = realNow
	q.vt += timesincelast * q.getvirtualtimeratio()
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
	return min(float64(reqs), float64(q.C)) / float64(NEQ)
}

func (q *FQScheduler) updateTime(packet *Packet, queue *Queue) {
	// When a request arrives to an empty queue with no requests executing
	// len(queue.Packets) == 1 as enqueue has just happened prior (vs  == 0)
	if len(queue.Packets) == 1 && queue.RequestsExecuting == 0 {
		// the queue’s virtual start time is set to getVirtualTime().
		queue.virstart = q.getVirtualTime()
	}
}

// FinishPacketAndDequeue is a convenience method used using the FQScheduler
// at the concurrency limit
func (q *FQScheduler) FinishPacketAndDeque(p *Packet) (*Packet, bool) {
	q.FinishPacket(p)
	return q.Dequeue()
}

// FinishPacket is a callback that should be used when a previously dequeud packet
// has completed it's service.  This callback updates imporatnt state in the
//  FQScheduler
func (q *FQScheduler) FinishPacket(p *Packet) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.synctime()
	S := q.clock.Since(p.startTime).Seconds()

	// When a request finishes being served, and the actual service time was S,
	// the queue’s virtual start time is decremented by G - S.
	q.queues[p.queueidx].virstart -= q.G - S

	// request has finished, remove from requests executing
	q.queues[p.queueidx].RequestsExecuting--
}

// Dequeue dequeues a packet from the fair queuing scheduler
func (q *FQScheduler) Dequeue() (*Packet, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.synctime()
	queue := q.selectQueue()

	if queue == nil {
		return nil, false
	}
	packet, ok := queue.Dequeue()

	if ok {
		// When a request is dequeued for service -> q.virstart += G
		queue.virstart += q.G

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
			curvirfinish := queue.VirtualFinish(0, q.G)
			if curvirfinish < minvirfinish {
				minvirfinish = curvirfinish
				minqueue = queue
				minidx = idx
			}
		}
	}
	q.robinidx = minidx
	return minqueue
}
