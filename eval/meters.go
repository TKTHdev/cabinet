package eval

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"strconv"
	"sync"
	"time"
)

type serverID = int
type prioClock = int
type priority = float64

type BatchMetrics struct {
	SlowPathCount int
	ConflictCount int
}

type PerfMeter struct {
	sync.RWMutex
	numOfTotalTx   int
	batchSize      int
	sampleInterval prioClock
	lastPClock     prioClock
	fileName       string
	meters         map[prioClock]*RecordInstance
	SlowCommits     int64
	ConflictCommits int64
	slowPathLatencySum float64
	slowPathCount      int
}

type RecordInstance struct {
	StartTime   time.Time
	TimeElapsed float64 // Changed from int64 to float64 to store precise values
	Metrics     BatchMetrics
}

func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
	m.meters = make(map[prioClock]*RecordInstance)
}

func (m *PerfMeter) RecordStarter(pClock prioClock) {
	m.RecordStarterAt(pClock, time.Now())
}

func (m *PerfMeter) RecordStarterAt(pClock prioClock, startTime time.Time) {
	m.Lock()
	defer m.Unlock()
	if startTime.IsZero() {
		startTime = time.Now()
	}

	m.meters[pClock] = &RecordInstance{
		StartTime:   startTime,
		TimeElapsed: 0.0,
		Metrics:     BatchMetrics{},
	}
}

func (m *PerfMeter) RecordFinisher(pClock prioClock) error {
	m.Lock()
	defer m.Unlock()

	_, exist := m.meters[pClock]
	if !exist {
		return errors.New("pClock has not been recorded with starter")
	}

	start := m.meters[pClock].StartTime
	m.meters[pClock].TimeElapsed = float64(time.Now().Sub(start).Microseconds()) / 1000.0 // Record precise milliseconds

	return nil
}

func (m *PerfMeter) IncConflict(pClock prioClock) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[pClock]; ok {
		rec.Metrics.ConflictCount++
	}
}

func (m *PerfMeter) IncSlowPath(pClock prioClock) {
	m.Lock()
	defer m.Unlock()
	if rec, ok := m.meters[pClock]; ok {
		rec.Metrics.SlowPathCount++
	}
}

func (pm *PerfMeter) RecordSlowCommit() {
	atomic.AddInt64(&pm.SlowCommits, 1)
}

func (pm *PerfMeter) AddSlowCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.SlowCommits, int64(n))
}

func (pm *PerfMeter) AddConflictCommits(n int) {
	if n <= 0 {
		return
	}
	atomic.AddInt64(&pm.ConflictCommits, int64(n))
}

func (m *PerfMeter) RecordSlowPathLatency(latencyMs float64) {
	m.Lock()
	defer m.Unlock()
	m.slowPathLatencySum += latencyMs
	m.slowPathCount++
}

func (m *PerfMeter) SaveToFile() error {
	totalSlow := atomic.LoadInt64(&m.SlowCommits)
	totalConflict := atomic.LoadInt64(&m.ConflictCommits)

	var folderName string

	if len(m.fileName) >= 6 && m.fileName[:6] == "client" {
		var clientPart string
		for i := 6; i < len(m.fileName); i++ {
			if m.fileName[i] == '_' {
				clientPart = m.fileName[:i]
				break
			}
		}
		if clientPart == "" {
			clientPart = m.fileName
		}
		folderName = clientPart
	} else if len(m.fileName) >= 1 && m.fileName[0] == 's' {
		folderName = fmt.Sprintf("server%d", m.fileName[1]-'0')
	} else {
		folderName = "default"
	}

	dirPath := fmt.Sprintf("./eval/%s", folderName)
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return err
	}

	timestamp := time.Now().Format("20060102_150405")
	filePath := fmt.Sprintf("%s/%s_%s.csv", dirPath, m.fileName, timestamp)

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	m.RLock()
	defer m.RUnlock()

	var keys []int
	for key := range m.meters {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx/sec)", "slow path ops", "conflict ops"})
	if err != nil {
		return err
	}

	counter := 0
	var latSum float64 = 0
	var tptSum float64 = 0
	var latencies []float64
	var slowSum, conflictSum int = 0, 0

	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}

		latSum += value.TimeElapsed
		counter++
		latencies = append(latencies, value.TimeElapsed)
		slowSum += value.Metrics.SlowPathCount
		conflictSum += value.Metrics.ConflictCount

		lat := value.TimeElapsed
		tpt := (float64(m.batchSize) / lat) * 1000
		tptSum += tpt

		row := []string{
			strconv.Itoa(key),
			strconv.FormatFloat(lat, 'f', 3, 64),
			strconv.FormatFloat(tpt, 'f', 3, 64),
			strconv.Itoa(value.Metrics.SlowPathCount),
			strconv.Itoa(value.Metrics.ConflictCount),
		}

		err := writer.Write(row)
		if err != nil {
			return err
		}
	}

	if counter == 0 {
		// No completed batches recorded. Write a NO_DATA row and still emit global totals
		_ = writer.Write([]string{"NO_DATA", "", "", "", ""})
		// Still write global totals (likely zeros)
		err = writer.Write([]string{
			"GLOBAL_TOTALS",
			"",
			"",
			strconv.FormatInt(totalSlow, 10) + " ops",
			strconv.FormatInt(totalConflict, 10) + " ops",
		})
		if err != nil {
			return err
		}
		return nil
	}

	avgLatency := float64(latSum) / float64(counter)
	// Calculate actual wall-clock throughput
	firstBatch := m.meters[keys[0]]
	lastBatch := m.meters[keys[len(keys)-1]]
	lastEndTime := lastBatch.StartTime.Add(time.Duration(lastBatch.TimeElapsed) * time.Millisecond)
	totalWallClockSeconds := lastEndTime.Sub(firstBatch.StartTime).Seconds()

	// This is your TRUE throughput (Tx/sec)
	actualThroughput := float64(m.batchSize*counter) / totalWallClockSeconds

	avgSlow := float64(slowSum) / float64(counter)
	avgConflict := float64(conflictSum) / float64(counter)

	sort.Float64s(latencies)
	p50Latency := latencies[len(latencies)*50/100]
	p95Latency := latencies[len(latencies)*95/100]
	p99Latency := latencies[len(latencies)*99/100]
	avgLatencyPerTx := avgLatency / float64(m.batchSize)

	// Write summary row with averages
	err = writer.Write([]string{
		"AVERAGE",
		strconv.FormatFloat(avgLatency, 'f', 3, 64) + " ms",
		strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
		strconv.FormatFloat(avgSlow, 'f', 3, 64),
		strconv.FormatFloat(avgConflict, 'f', 3, 64),
	})
	if err != nil {
		return err
	}

	// Write overall throughput (total ops / total time)
	err = writer.Write([]string{
		"THROUGHPUT",
		"",
		strconv.FormatFloat(actualThroughput, 'f', 3, 64) + " Tx/sec",
		"",
		"",
	})
	if err != nil {
		return err
	}


	err = writer.Write([]string{"P50_LATENCY", strconv.FormatFloat(p50Latency, 'f', 0, 64) + " ms", "", "", ""})
	if err != nil {
		return err
	}

	err = writer.Write([]string{"P95_LATENCY", strconv.FormatFloat(p95Latency, 'f', 0, 64) + " ms", "", "", ""})
	if err != nil {
		return err
	}

	err = writer.Write([]string{"P99_LATENCY", strconv.FormatFloat(p99Latency, 'f', 0, 64) + " ms", "", "", ""})
	if err != nil {
		return err
	}

	err = writer.Write([]string{"AVG_LATENCY_PER_TX", strconv.FormatFloat(avgLatencyPerTx, 'f', 3, 64) + " ms/Tx", "", "", ""})
	if err != nil {
		return err
	}

	// Write global totals row (operation counts, not batch counts)
	err = writer.Write([]string{
		"GLOBAL_TOTALS",
		"",
		"",
		strconv.FormatInt(totalSlow, 10) + " ops",
		strconv.FormatInt(totalConflict, 10) + " ops",
	})
	if err != nil {
		return err
	}

	err = writer.Write([]string{"TOTAL_SLOW_COMMITS", strconv.FormatInt(totalSlow, 10), "ops"})
	if err != nil {
		return err
	}
	err = writer.Write([]string{"TOTAL_CONFLICT_COMMITS", strconv.FormatInt(totalConflict, 10), "ops"})
	if err != nil {
		return err
	}

	return nil
}
