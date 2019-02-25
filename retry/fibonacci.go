package retry

func Fibonacci() func() int {
	a, b := 0, 1
	return func() int {
		a, b = b, a+b
		return a
	}
}

func FibonacciNext(start int) int {
	fib := Fibonacci()
	num := fib()
	for num <= start {
		num = fib()
	}
	return num
}
