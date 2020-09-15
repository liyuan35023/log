package gopool

import (
	"errors"
	"time"
)
// Job is user submit work, will be executed by worker in worker pool
type Job func()
// worker goroutine, accept job and execute.
type worker struct {
	workerId   int64
	workerPool chan *worker
	workerJobQ chan Job
	shutdown   chan interface{}
}
func newWorker(workerPool chan *worker, jobQ chan Job) *worker {
	return &worker{
		workerPool: workerPool,
		workerJobQ: make(chan Job),
		shutdown:   make(chan interface{}),
	}
}
func (w *worker) start() {
	go func() {
		for {
			// finish job or worker start. put worker back to worker pool after work finish a job.
			w.workerPool <- w
			select {
			case job := <-w.workerJobQ:
				// fighting! do job.
				job()
			case <-w.shutdown:
				// get off work, go home play game.
				return
			}
		}
	}()
}
func (w *worker) stop() {
	close(w.shutdown)
}
// accept job, dispatch jop to worker when worker pool has free worker.
// now implement a simple policy to dispatch job
type jobDispatcher struct {
	workerNum       int
	workerPool      chan *worker
	jobQ            chan Job
	shutdown        chan interface{}
	shutdownFinsish chan interface{}
}
func newJobDispatcher(workerNum int, jobQ chan Job) *jobDispatcher {
	workerPool := make(chan *worker, workerNum)
	for i := 0; i < workerNum; i++ {
		worker := newWorker(workerPool, jobQ)
		worker.workerId = int64(i)
		worker.start()
	}
	return &jobDispatcher{
		workerNum:       workerNum,
		workerPool:      workerPool,
		jobQ:            jobQ,
		shutdown:        make(chan interface{}),
		shutdownFinsish: make(chan interface{}),
	}
}
func (dispatcher *jobDispatcher) dispatch() {
	for {
		select {
		case job := <-dispatcher.jobQ:
			worker := <-dispatcher.workerPool
			worker.workerJobQ <- job
		case <-dispatcher.shutdown:
			// wait all worker finish, send shutdown signal
			for i := 0; i < dispatcher.workerNum; i++ {
				worker := <-dispatcher.workerPool
				worker.stop()
				// todo: need clear jobQ ?
			}
			close(dispatcher.shutdownFinsish)
			return
		}
	}
}
func (dispatcher *jobDispatcher) stop() {
	close(dispatcher.shutdown)
	<-dispatcher.shutdownFinsish
}
// GoPool provide a goroutine pool.
type GoPool struct {
	dispatcher *jobDispatcher
	jobQ       chan Job
}
func NewGoPool(workerNum, jobQueenSize int) *GoPool {
	jobQ := make(chan Job, jobQueenSize)
	dispatcher := newJobDispatcher(workerNum, jobQ)
	// start dispatch
	go dispatcher.dispatch()
	return &GoPool{
		dispatcher: dispatcher,
		jobQ:       jobQ,
	}
}
// Submit submit a job, do not support timeout, so you should do this in you own code or function.
func (gp *GoPool) Submit(job Job) {
	gp.jobQ <- job
}
// Submit submit a job, if jobQ is full, and then will wait until timeout. if timeout, will return error
func (gp *GoPool) SubmitWithTimeout(job Job, timeout time.Duration) error {
	for {
		select {
		case gp.jobQ <- job:
			return nil
		case <-time.After(timeout):
			return errors.New("job queue is full, timeout")
		}
	}
}
// JobQueue return job queue for you, so you can control job submit, waitgroup, timeout yourself.
func (gp *GoPool) JobQueue() chan Job {
	return gp.jobQ
}
// ShutDown stop all worker, and wait job in execute finish, jump out.
func (gp *GoPool) ShutDown() {
	gp.dispatcher.stop()
}
