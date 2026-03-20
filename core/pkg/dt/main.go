/**
 * @Author:         yi
 * @Description:    初始化
 * @Version:        1.0.0
 * @Date:           2025/2/11 17:12
 */
package dt

import "context"

type debounceType struct {
	Call     func()
	ExecTime int64
}

type throttledType struct {
	Call   func()
	Cancel context.CancelFunc
}

func init() {
	go debounceRunner()
	go throttleFixedGridIdleRunner()
}
