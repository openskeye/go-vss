// @Title        dt_test
// @Description  防抖/延迟/周期 行为校验
// @Create       assistant

package dt

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestThrottleFixedGridTrailing_ScaledThreeSlots(t *testing.T) {
	var (
		period = 50 * time.Millisecond
		count  int32
		start  = time.Now()
	)

	for time.Since(start) < 102*time.Millisecond {
		ThrottleFixedGridTrailing("grid-scale", period, func() {
			atomic.AddInt32(&count, 1)
		})
	}

	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&count) != 3 {
		t.Fatalf("期望 102ms 内按 50ms 槽对齐共触发 3 次，实际 %d", count)
	}
}

func TestThrottleFixedGridTrailing_RemovesIdleFromMap(t *testing.T) {
	var (
		key = "cleanup-idle-key"
		n   int32
	)

	ThrottleFixedGridTrailing(key, 40*time.Millisecond, func() {
		atomic.AddInt32(&n, 1)
	})

	time.Sleep(120 * time.Millisecond)

	if throttleFixedGridMaps.Has(key) {
		t.Fatal("期望槽尾执行完毕后空闲条目已从 throttleFixedGridMaps 移除")
	}

	if atomic.LoadInt32(&n) != 1 {
		t.Fatalf("期望回调执行 1 次，实际 %d", n)
	}
}

func TestThrottleFixedGridTrailing_LastInSlotWins(t *testing.T) {
	var (
		period = 80 * time.Millisecond
		last   int32
	)

	for i := 0; i < 20; i++ {
		var v = int32(i)

		ThrottleFixedGridTrailing("g-last", period, func() {
			atomic.StoreInt32(&last, v)
		})
	}

	time.Sleep(period + 50*time.Millisecond)

	if atomic.LoadInt32(&last) != 19 {
		t.Fatalf("期望槽内保留最后一次回调的值 19，实际 %d", last)
	}
}

func TestThrottled_OnlyLastWindowFires(t *testing.T) {
	var count int32

	TrailingDebounce("th-test", 80*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	TrailingDebounce("th-test", 80*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(40 * time.Millisecond)

	TrailingDebounce("th-test", 80*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("期望 TrailingDebounce 在多次触发后仅执行 1 次，实际执行 %d 次", count)
	}
}

func TestThrottled_StaleTimerDoesNotFireReplacedEntry(t *testing.T) {
	var first, second int32

	TrailingDebounce("race-key", 100*time.Millisecond, func() {
		atomic.StoreInt32(&first, 1)
	})

	time.Sleep(30 * time.Millisecond)

	TrailingDebounce("race-key", 100*time.Millisecond, func() {
		atomic.StoreInt32(&second, 1)
	})

	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&first) != 0 {
		t.Fatal("期望已被替换的调度不执行第一次回调")
	}

	if atomic.LoadInt32(&second) != 1 {
		t.Fatal("期望仅最后一次调度在窗口结束后执行")
	}
}

func TestDebounce_ResetsDeadline(t *testing.T) {
	var count int32

	Debounce("db-test", 100*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(50 * time.Millisecond)

	Debounce("db-test", 100*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	// 第二次调用后仅过 60ms，未到「当前时刻 + 100ms」截止时间
	time.Sleep(60 * time.Millisecond)

	if atomic.LoadInt32(&count) != 0 {
		t.Fatalf("期望再次 Debounce 会推迟执行，此时不应已触发，实际 count=%d", count)
	}

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("期望静默结束后执行 1 次，实际 count=%d", count)
	}
}

func TestThrottle_LeadingEdgeDropsCallsInWindow(t *testing.T) {
	var count int32

	Throttle("thr-test", 100*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	Throttle("thr-test", 100*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	Throttle("thr-test", 100*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(30 * time.Millisecond)

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("期望节流窗口内多次触发仅执行 1 次，实际 %d", count)
	}

	time.Sleep(100 * time.Millisecond)

	Throttle("thr-test", 100*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(30 * time.Millisecond)

	if atomic.LoadInt32(&count) != 2 {
		t.Fatalf("期望窗口结束后可再次触发，期望 count=2，实际 %d", count)
	}
}

func TestSetTimeout_CancelStopsCallback(t *testing.T) {
	var fired int32

	var cancel = SetTimeout(100*time.Millisecond, func() {
		atomic.StoreInt32(&fired, 1)
	})

	cancel()

	time.Sleep(150 * time.Millisecond)

	if atomic.LoadInt32(&fired) != 0 {
		t.Fatal("期望取消 SetTimeout 后回调不执行")
	}
}

func TestSetInterval_RepeatsUntilCancel(t *testing.T) {
	var count int32

	var cancel = SetInterval(40*time.Millisecond, func() {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(130 * time.Millisecond)

	cancel()

	time.Sleep(80 * time.Millisecond)

	var n = atomic.LoadInt32(&count)

	if n < 2 {
		t.Fatalf("期望 SetInterval 在时间窗内至少触发 2 次，实际 %d", n)
	}
}
