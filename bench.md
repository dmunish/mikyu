# Before batched fsync
goos: windows
goarch: amd64
pkg: github.com/dmunish/mikyu/queue
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkProduceSerial-8             945           1492143 ns/op             424 B/op          4 allocs/op
BenchmarkProduceParallel-8           889           1362259 ns/op            1298 B/op          5 allocs/op
PASS
ok      github.com/dmunish/mikyu/queue  3.836s

# WAL batched fsync (fixed batchsize/time strategy)
goos: windows
goarch: amd64
pkg: github.com/dmunish/mikyu/queue
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkProduceSerial-8              10         100215810 ns/op             783 B/op          8 allocs/op
BenchmarkProduceParallel-8            86          12840298 ns/op            3484 B/op          6 allocs/op
PASS
ok      github.com/dmunish/mikyu/queue  4.689s

# WAL batched fsync (dynamic strategy optimizing for busy + idle workloads)
goos: windows
goarch: amd64
pkg: github.com/dmunish/mikyu/queue
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkProduceSerial-8             820           1358001 ns/op             587 B/op          6 allocs/op
BenchmarkProduceParallel-8          3655            291459 ns/op            3189 B/op          5 allocs/op
PASS
ok      github.com/dmunish/mikyu/queue  3.201s

# WAL segmentation
goos: windows
goarch: amd64
pkg: github.com/dmunish/mikyu/queue
cpu: Intel(R) Core(TM) i5-8350U CPU @ 1.70GHz
BenchmarkProduceSerial-8             838           1394581 ns/op             634 B/op          7 allocs/op
BenchmarkProduceParallel-8          4782            217762 ns/op            3334 B/op          6 allocs/op
PASS
ok      github.com/dmunish/mikyu/queue  4.226s