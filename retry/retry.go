package retry

import (
	"fmt"
	"github.com/itzujun/GoCelery/retry"
	"time"
)

var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := retry.Fibonacci()

	return func(stopChan chan int) {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, _ := time.ParseDuration(durationString)
			select {
			case <-stopChan:
				break
			case <-time.After(duration):
				break
			}
		}
		retryIn = fibonacci()
	}
}
