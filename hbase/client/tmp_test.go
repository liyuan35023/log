package client

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	timer2 := time.NewTimer(time.Second)
	go func() {
		<-timer2.C
		fmt.Println("Timer 2 expired")
	}()
	stop2 := timer2.Stop()
	if stop2 {
		fmt.Println("Timer 2 stopped")
	}
}

func TestSlice(t *testing.T) {
	var slice []string
	slice = nil
	fmt.Println(len(slice))
}

func TestAbort(t *testing.T) {
	abortCh := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for {
			select {
			case <-abortCh:
				fmt.Println("------------> abort")
				return
			default:
				time.Sleep(4 * time.Second)
				count++
				fmt.Println("------------->", count)
				return
			}
		}
	}()

	time.Sleep(2 * time.Second)

	close(abortCh)
	wg.Wait()
}

func TestCancelContext(t *testing.T) {
	// gen generates integers in a separate goroutine and
	// sends them to the returned channel.
	// The callers of gen need to cancel the context once
	// they are done consuming generated integers not to leak
	// the internal goroutine started by gen.
	gen := func(ctx context.Context) <-chan int {
		dst := make(chan int)
		n := 1
		go func() {
			for {
				select {
				case <-ctx.Done():
					return // returning not to leak the goroutine
				case dst <- n:
					n++
				}
			}
		}()
		return dst
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancel when we are finished consuming integers

	for n := range gen(ctx) {
		fmt.Println(n)
		if n == 5 {
			break
		}
	}
}

func TestTimeoutContext(t *testing.T) {
	// Pass a context with a timeout to tell a blocking function that it
	// should abandon its work after the timeout elapses.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	select {
	case <-time.After(1 * time.Second):
		fmt.Println("overslept")
	case <-ctx.Done():
		fmt.Println(ctx.Err()) // prints "context deadline exceeded"
	}
}

func TestChannelRange(t *testing.T) {
	numCh := make(chan int)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			for n := range numCh {
				fmt.Println(num, "num: ", n)
			}
		}(i)
	}

	for j := 0; j < 50; j++ {
		numCh <- j
	}

	close(numCh)

	wg.Wait()
}

type TestCopy struct {
	num int
}

func TestCopyFunc(t *testing.T) {
	obj1 := &TestCopy{num: 1}
	obj2 := obj1
	obj2.num = 3

	fmt.Println(obj1.num, obj2.num)
}

func TestRune(t *testing.T) {
	for ch := 'A'; ch < 'A'+rune(40); ch++ {
		server := "servers" + string(ch)
		fmt.Println(server)
	}
}

func TestSlotNum(t *testing.T) {
	fmt.Println(1 << 14)
}
