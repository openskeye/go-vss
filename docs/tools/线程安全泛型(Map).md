# xmap - 线程安全的泛型Map库

参考代码 [点击直达](https://github.com/openskeye/go-vss/blob/main/core/pkg/xmap)

## 简介

`xmap` 是一个基于Go泛型实现的线程安全Map库，提供了丰富的操作方法，适用于并发场景下的键值对存储需求。通过读写锁(RWMutex)
保证并发安全，同时利用泛型特性支持任意可比较的键类型和任意值类型。

## 特性

- **线程安全**：使用读写锁(RWMutex)保证并发安全
- **泛型支持**：支持任意可比较的键类型和任意值类型
- **丰富API**：提供完整的增删改查操作方法
- **性能优化**：支持初始化容量设置，减少map扩容开销
- **实用功能**：提供遍历、条件设置等便捷方法

## 快速开始

创建XMap实例

```
// 创建一个初始容量为10的XMap
var m = xmap.New[string, int](10)
```

## 基本操作

```
// 设置键值对
m.Set("age", 25)
m.Set("score", 100)

// 获取值
if val, ok := m.Get("age"); ok {
    fmt.Println("age:", val)
}

// 检查键是否存在
if m.Contains("name") {
    fmt.Println("name exists")
}

// 获取元素个数
fmt.Println("size:", m.Len())

// 删除键
m.Remove("score")
fmt.Println("size:", m.Len())

// 清空所有元素
m.Clear()
fmt.Println("size:", m.Len())
```

## 类型定义

**XMap[K, V]** 泛型线程安全的Map结构体。

- K：键类型，必须实现comparable接口
- V：值类型，可以是任意类型

**RecordType[K, V]**

键值对记录类型，用于返回键值对组合。

```
type RecordType[K comparable, V any] struct {
    Key   K
    Value V
}
```

## 构造函数

`New[K comparable, V any](size int) *XMap[K, V]`

创建指定初始容量的XMap实例。

- 参数：
  - `size` 初始容量，用于底层map的初始化

- 返回值：
  - *XMap[K, V]：XMap实例指针

## 核心方法

### `Set(key K, value V)`

设置键值对。如果键已存在，则更新对应的值。

```
m.Set("name", "张三")
m.Set("age", 30)
```

### `Get(key K) (V, bool)`

获取指定键的值。

返回值：

- V：键对应的值
- bool：键是否存在

```
if val, ok := m.Get("name"); ok {
    fmt.Println(val)
}
```

### `Remove(key K)`

删除指定键及其对应的值。

```
m.Remove("name")
```

### `Clear()`

清空Map中的所有数据。重新分配新的map，保持初始容量。

```
m.Clear()
```

## 查询方法

### `Keys() []K`

返回所有键的切片。

```
var keys = m.Keys()
for _, k := range keys {
    fmt.Println(k)
}
```

### `Values() []V`

返回所有值的切片。

```
var values = m.Values()
for _, v := range values {
    fmt.Println(v)
}
```

### `Records() []*RecordType[K, V]`

返回所有键值对记录的切片。

```
var records = m.Records()
for _, r := range records {
    fmt.Printf("Key: %v, Value: %v\n", r.Key, r.Value)
}
```

### `All() map[K]V`

返回底层map。注意：返回的是原始map引用，修改会影响内部数据。

```
var data = m.All()
data["new"] = 100 // 会修改原始数据
```

### `AllCopy() map[K]V`

返回底层map的副本。修改副本不会影响内部数据。

```
var data = m.AllCopy()
data["new"] = 100 // 不会影响原始数据
```

### `Len() int`

返回当前元素个数。

```
size := m.Len()
```

### `Contains(key K) bool`

检查指定键是否存在。

```
if m.Contains("name") {
    fmt.Println("键存在")
}
```

## 实用方法

### `GetOrSet(key K, defaultValue V) V`

获取指定键的值，如果键不存在则设置默认值并返回。

```
// 如果"count"不存在，则设置为0并返回0
count := m.GetOrSet("count", 0)
```

### `SetIfAbsent(key K, value V) bool`

如果键不存在则设置值。

返回值：

- true：设置成功（键不存在）
- false：设置失败（键已存在）

```
if m.SetIfAbsent("name", "李四") {
    fmt.Println("设置成功")
} else {
    fmt.Println("键已存在")
}
```

### `ForEach(fn func(key K, value V))`

遍历所有元素，对每个键值对执行指定函数。

```
m.ForEach(func(key string, value int) {
    fmt.Printf("Key: %s, Value: %d\n", key, value)
})
```

## 性能优化

- 初始化容量：通过New(size)指定合适的初始容量，可以减少map扩容带来的性能开销
- 读多写少场景：使用读写锁，多个goroutine可以同时读取
- 返回副本：AllCopy()方法返回map副本，避免外部修改影响内部数据

## 注意事项

- All()方法返回的map是内部数据的直接引用，修改会影响原始数据
- 迭代顺序不保证与插入顺序一致
- 在遍历过程中（ForEach）不允许修改map结构（增删元素）
