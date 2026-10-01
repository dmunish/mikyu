package result

// SeriesRow is one per-second observation; column order is the CSV header order.
type SeriesRow struct {
	T      float64    `json:"t"`
	Phase  string     `json:"phase"`
	Client ClientCols `json:"client"`
	System SystemCols `json:"system"`
	Host   HostCols   `json:"host"`
}

// ClientCols holds the observations the bench client itself owns.
type ClientCols struct {
	MsgsInFlight int64   `json:"msgs_in_flight"`
	Throughput   float64 `json:"throughput_msgs_per_sec"`
	P50NS        int64   `json:"p50_ns"`
	P90NS        int64   `json:"p90_ns"`
	P99NS        int64   `json:"p99_ns"`
}

// SystemCols holds the observations the queue's storage reports.
type SystemCols struct {
	QueueDepth     int64   `json:"queue_depth,omitempty"`
	Segments       int64   `json:"segments,omitempty"`
	SizeBytes      int64   `json:"size_bytes,omitempty"`
	CacheHits      int64   `json:"cache_hits,omitempty"`
	CacheMisses    int64   `json:"cache_misses,omitempty"`
	CacheBytes     int64   `json:"cache_bytes,omitempty"`
	WALChannelFill float64 `json:"wal_channel_fill,omitempty"`
	WALByteFill    float64 `json:"wal_byte_fill,omitempty"`
}

// HostCols holds the observations about the bench process itself.
type HostCols struct {
	RSSBytes   int64   `json:"rss_bytes,omitempty"`
	CPUPercent float64 `json:"cpu_percent,omitempty"`
	OpenFDs    int64   `json:"open_fds,omitempty"`
	Threads    int64   `json:"threads,omitempty"`
}

// seriesHeader is the fixed column order of series.csv; never reorder, only append.
const seriesHeader = "t,phase," +
	"client_msgs_in_flight,client_throughput_msgs_per_sec,client_p50_ns,client_p90_ns,client_p99_ns," +
	"system_queue_depth,system_segments,system_size_bytes,system_cache_hits,system_cache_misses," +
	"system_cache_bytes,system_wal_channel_fill,system_wal_byte_fill," +
	"host_rss_bytes,host_cpu_percent,host_open_fds,host_threads\n"
