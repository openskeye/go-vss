// @Title        main
// @Description  main
// @Create       yiyiyi 2025/9/8 16:50

package broadcast

import (
	"sync"
	"time"
)

type BroadcastManager struct {
	channels    sync.Map // channelName -> *channelInfo
	maxCapacity int      // 每个channel最大容量
}

type channelInfo struct {
	ch        chan interface{}
	createdAt time.Time
	usage     int // 使用计数
}

func NewBroadcast(maxCapacity int) *BroadcastManager {
	return &BroadcastManager{
		maxCapacity: maxCapacity,
	}
}

// 发送数据，如果channel不存在或已满，则丢弃数据
func (cm *BroadcastManager) Send(channelName string, data interface{}) bool {
	// 检查channel是否存在
	actual, exists := cm.channels.Load(channelName)
	if !exists {
		return false
	}

	var info = actual.(*channelInfo)
	info.usage++

	select {
	case info.ch <- data:
		return true
	default:
		// channel已满，丢弃数据
		return false
	}
}

// 注册接收者，创建channel
func (cm *BroadcastManager) RegisterReceiver(channelName string) <-chan interface{} {
	var info = &channelInfo{
		ch:        make(chan interface{}, cm.maxCapacity),
		createdAt: time.Now(),
		usage:     0,
	}

	actual, loaded := cm.channels.LoadOrStore(channelName, info)
	if loaded {
		// channel已存在，返回现有的
		return actual.(*channelInfo).ch
	}

	return info.ch
}

// 取消注册，清理channel
func (cm *BroadcastManager) UnregisterReceiver(channelName string) {
	if actual, exists := cm.channels.Load(channelName); exists {
		var info = actual.(*channelInfo)
		close(info.ch)
		cm.channels.Delete(channelName)

		// 清理channel中剩余的数据
		go cm.cleanupChannel(info.ch, channelName)
	}
}

// 清理channel中剩余的数据
func (cm *BroadcastManager) cleanupChannel(ch <-chan interface{}, channelName string) {
	var dropped = 0
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
			dropped++
		default:
			return
		}
	}
}

// 自动清理长时间未使用的channel
func (cm *BroadcastManager) StartCleanupWorker(interval time.Duration, maxIdle time.Duration) {
	go func() {
		var ticker = time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			var now = time.Now()
			cm.channels.Range(func(key, value interface{}) bool {
				var (
					channelName = key.(string)
					info        = value.(*channelInfo)
				)
				if now.Sub(info.createdAt) > maxIdle && info.usage == 0 {
					cm.UnregisterReceiver(channelName)
				}
				return true
			})
		}
	}()
}
