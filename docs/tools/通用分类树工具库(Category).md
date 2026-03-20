# 通用分类树工具库：从设计到使用

[//]: # (源码文件: core/pkg/categories/main.go)

> 一篇关于如何设计和实现一个泛型、高性能、易用的分类树处理工具库的完整技术分享

## 本文目录

- 写在前面

- 一、设计目标：我们要解决什么问题？

- 二、核心数据结构设计

- 三、核心功能实现

- 四、使用示例

- 五、性能优化与注意事项

- 六、总结

---

在业务系统开发中，分类树（Category Tree）是一个非常常见的需求：<br>
商品分类、组织架构、行政区划、菜单权限...几乎每个项目都会遇到。：<br>
但每次都要重复写递归找子节点、找父节点、列表转树形结构这些代码，不仅繁琐，还容易出错。<br>
本文将分享一个泛型分类树工具库的设计与实现过程，它具备以下特点：

- 泛型支持：ID 可以是 int 或 string，数据可以是任意类型
- 零依赖：纯标准库实现，开箱即用
- 功能完整：列表转树、树转列表、查找子树、查找祖级、查找特定节点
- 类型安全：充分利用 Go 泛型，编译时保证类型正确

## 一、设计目标：我们要解决什么问题？

在开始写代码之前，先明确我们要解决的核心问题：

### 1.1 常见的分类树操作需求

```
// 假设我们有这样的分类数据
type Category struct {
    ID   int
    Pid  int
    Name string
}

var categories = []Category{
    {ID: 1, Pid: 0, Name: "数码"},
    {ID: 2, Pid: 1, Name: "手机"},
    {ID: 3, Pid: 2, Name: "iPhone"},
    // ...
}
```

我们需要的能力：

- 列表 → 树：将平铺的列表转换为树形结构（用于前端展示）
- 树 → 列表：将树形结构重新平铺（用于数据导出）
- 查找子树：给定一个节点，找到它下面所有的子节点（含自身）
- 查找祖级：给定一个节点，找到它上面所有的父节点
- 查找节点：在树中查找特定 ID 的节点

### 1.2 设计

- ID 类型不确定：可能是 int（可以拓展为 **comparable** ），可能是 string
- 数据类型不确定：可能是商品分类，可能是部门，可能是区域
- 性能要求：频繁的递归查找不能太慢
- 易用性：API 要简单直观

---

## 二、核心数据结构设计

### 2.1 Item：树的节点定义

```
type (
    // keyType 约束 ID 的类型只能是 int 或 string (可以改为comparable)
    keyType interface {
        int | string
    }

    // Item 树的节点
    Item[T keyType, D any] struct {
        ID       T             `json:"id"`                 // 节点ID
        Pid      T             `json:"pid"`                 // 父节点ID
        Name     string        `json:"name"`                // 节点名称
        Raw      D             `json:"raw,omitempty,optional"` // 原始数据
        Children []*Item[T, D] `json:"children,omitempty,optional"` // 子节点
    }
)
```

设计思路：

- 使用泛型
    - **T**支持灵活的 **ID**类型
    - **D**承载任意业务数据
- **Children**是切片，保持顺序性
- **Raw**字段可选，用于携带原始业务对象

### 2.2 Category：工具类的主体

```
type Category[T keyType, D any] struct {
    List  []*Item[T, D] // 平铺列表（原始数据）
    Trees []*Item[T, D] // 树形结构（根节点列表）
}
```

设计思路：

- 同时维护列表和树，避免重复计算
- 列表用于快速查找和祖级追溯
- 树用于树形操作和子节点查找

---

## 三、核心功能实现

### 3.1 列表转树（Conv）

这是最核心的功能：将平铺的列表转换为树形结构。

```
// Conv 转换列表为分类树
func (c *Category[T, D]) Conv(list []D, call func(D) *Item[T, D]) *Category[T, D] {
    // 1. 转换列表
    var length = len(list)
    c.List = make([]*Item[T, D], length)
    for key, item := range list {
        var v = call(item)
        
        // 2. 过滤无效ID
        if any(v.ID) == nil {
            continue
        }
        
        // 处理 string 类型的空值
        if tmp, ok := any(v.ID).(string); ok {
            if tmp == "" {
              continue
            }
        }
        
        // 处理 int 类型的零值
        if tmp, ok := any(v.ID).(int); ok {
            if tmp tmp == 0 {
                continue
            }
        }

        c.List[key] = v
    }

    if len(c.List) <= 0 {
        return c
    }

    // TODO 如果使用comparable这里要注意区分 comparable/keyType
    // 3. 构建树形结构（根节点ID为0）
    c.Trees = c.makeTrees(T(0))
    return c
}
```

关键细节：

- 通过回调函数让调用方决定如何构建 Item
- 自动过滤无效 ID（nil、空字符串、0值）
- 根节点的 PID 约定为 0（或空字符串）

### 3.2 递归构建树（makeTrees）

```
// makeTrees 递归构建树形结构
func (c *Category[T, D]) makeTrees(pid T) []*Item[T, D] {
    var children []*Item[T, D]
    for _, item := range c.List {
        var value = new(Item[T, D])
        *value = *item  // 值拷贝，避免相互影响
        
        if value.Pid == pid {
            children = append(children, value)
            // 递归找子节点
            value.Children = c.makeTrees(value.ID)
        }
    }

    if len(children) <= 0 {
        children = []*Item[T, D]{}
    }

    return children
}
```

设计要点：

- **值拷贝**：**\*value = \*item** 确保修改子节点不影响原列表
- **递归终止**：当没有子节点时返回空切片
- **深度优先**：先找子节点，再找孙节点

### 3.3 树转列表（SubFlatList）

```
// SubFlatList 树状结构转平铺列表
func (c *Category[T, D]) SubFlatList(trees []*Item[T, D]) []*Item[T, D] {
    var list []*Item[T, D]
    for _, item := range trees {
        var val = new(Item[T, D])
        *val = *item
        val.Children = nil  // 平铺时清除子节点
        
        list = append(list, val)
        if len(item.Children) > 0 {
            list = append(list, c.SubFlatList(item.Children)...)
        }
    }
    return list
}
```

应用场景：

- 数据库存储
- 数据导出
- 序列化传输

### 3.4 查找子树（FindTrees）

```
// FindTrees 查找指定ID下所有子集（包含自身）
func (c *Category[T, D]) FindTrees(parentIds []T, list []D, call func(D) *Item[T, D]) []T {
    var trees = c.Conv(list, call).Trees
    if len(trees) <= 0 {
        return nil
    }

    var (
        ids     = append([]T{}, parentIds...)
        records []*Item[T, D]
    )
    
    // 1. 找到所有父节点
    for _, id := range parentIds {
        var item = c.FindId(id, trees)
        if item == nil {
            continue
        }
        records = append(records, item)
    }

    // 2. 收集所有子节点ID
    for _, item := range records {
        for _, subItem := range c.SubFlatList(item.Children) {
            ids = append(ids, subItem.ID)
        }
    }

    return ids
}
```

典型场景：

- 删除分类时找出所有子分类
- 权限继承时找出所有子菜单
- 统计某个节点下所有数据

### 3.5 查找祖级（FindParents）

```
// FindParents 查找祖级节点
func (c *Category[T, D]) FindParents(ID T) []*Item[T, D] {
    // 1. 找到当前节点
    var current *Item[T, D]
    for _, item := range c.List {
        if item.ID == ID {
            current = new(Item[T, D])
            *current = *item
        }
    }

    if current == nil {
        return nil
    }

    var data = []*Item[T, D]{current}
    
    // 2. 检查是否为根节点
    if pid, ok := any(current.Pid).(string); ok && pid == "" {
        return data
    }
    if pid, ok := any(current.Pid).(int); ok && pid == 0 {
        return data
    }

    // 3. 递归找父节点并反转顺序（从上到下）
    var (
        list  = append([]*Item[T, D]{current}, c.findParents(current.Pid)...)
        res   = make([]*Item[T, D], len(list))
        index = 0
    )
    for i := len(list) - 1; i >= 0; i-- {
        res[index] = list[i]
        index += 1
    }

    return res
}

func (c *Category[T, D]) findParents(pid T) []*Item[T, D] {
    var parents []*Item[T, D]
    for _, item := range c.List {
        if item.ID == pid {
            var val = new(Item[T, D])
            *val = *item
            parents = append(parents, val)

            // 递归终止条件
            if pid, ok := any(val.Pid).(string); ok && pid == "" {
                break
            }
            if pid, ok := any(val.Pid).(int); ok && pid == 0 {
                break
            }

            parents = append(parents, c.findParents(val.Pid)...)
        }
    }
    return parents
}
```

**返回值设计**：返回从根到当前节点的路径，便于面包屑导航。

### 3.6 查找节点（Find/FindId）

```
// Find 在树中查找指定ID的节点
func (c *Category[T, D]) Find(ID T) *Item[T, D] {
    return c.find(ID, c.Trees)
}

func (c *Category[T, D]) find(ID T, subs []*Item[T, D]) *Item[T, D] {
    for _, item := range subs {
        if item.ID == ID {
            return item
        }

        if len(item.Children) > 0 {
            if val := c.find(ID, item.Children); val != nil {
                return val
            }
        }
    }
    return nil
}

// FindId 在指定数据中查找节点（支持外部传入数据）
func (c *Category[T, D]) FindId(id T, data []*Item[T, D]) *Item[T, D] {
    for _, item := range data {
        if item.ID == id {
            return item
        }

        if len(item.Children) > 0 {
            var data = c.FindId(id, item.Children)
            if data != nil {
                return data
            }
        }
    }
    return nil
}
```

---

## 四、使用示例

### 4.1 基础用法

```
package main

import (
    "encoding/json"
    "fmt"
    "strings"
)

// 定义业务数据结构
type ProductCategory struct {
    ID    int
    Pid   int
    Name  string
    Level int // 业务特有字段
}

func main() {
    // 准备数据
    var data = []ProductCategory{
        {ID: 1, Pid: 0, Name: "数码", Level: 1},
        {ID: 2, Pid: 0, Name: "家电", Level: 1},
        {ID: 3, Pid: 1, Name: "手机", Level: 2},
        {ID: 4, Pid: 3, Name: "iPhone", Level: 3},
        {ID: 5, Pid: 3, Name: "华为", Level: 3},
        {ID: 6, Pid: 2, Name: "电视", Level: 2},
    }

    // 创建分类树
    var category = New[int, ProductCategory]().Conv(
        data,
        func(item ProductCategory) *Item[int, ProductCategory] {
            return &Item[int, ProductCategory]{
                ID:   item.ID,
                Pid:  item.Pid,
                Name: item.Name,
                Raw:  item, // 携带原始数据
            }
        },
    )

    // 查看树形结构
    treesJSON, _ := json.MarshalIndent(category.Trees, "", "  ")
    fmt.Printf("树形结构：\n%s\n", treesJSON)

    // 查找 iPhone 的祖级路径
    var (
        parents := category.FindParents(4)
        names []string
    )
    for _, p := range parents {
        names = append(names, p.Name)
    }
    fmt.Printf("iPhone 的路径：%s\n", strings.Join(names, " > "))
    
    // 在树中查找节点
    var node = category.Find(3)
    if node != nil {
        fmt.Printf("找到节点：ID=%d, Name=%s\n", node.ID, node.Name)
    }
}
```

### 4.2 实际业务场景

```
// 场景1：删除分类时找出所有子分类ID
func DeleteCategory(cateID int) {
    // 获取所有分类（从数据库）
    var allCategories []ProductCategory
    db.Find(&allCategories)
    
    var cate = New[int, ProductCategory]().Conv(
        allCategories,
        func(item ProductCategory) *Item[int, ProductCategory] {
            return &Item[int, ProductCategory]{ID: item.ID, Pid: item.Pid}
        },
    )
    
    // 找出所有要删除的ID（包含子分类）
    var idsToDelete = cate.FindTrees([]int{cateID}, allCategories, 
        func(item ProductCategory) *Item[int, ProductCategory] {
            return &Item[int, ProductCategory]{ID: item.ID, Pid: item.Pid}
        },
    )
    
    // 批量删除
    db.Where("id IN ?", idsToDelete).Delete(&ProductCategory{})
}

// 场景2：构建前端级联选择器数据
type CascaderOption struct {
    Value    int              `json:"value"`
    Label    string           `json:"label"`
    Children []CascaderOption `json:"children,omitempty"`
}

func BuildCascaderOptions() []CascaderOption {
    var all []ProductCategory
    db.Find(&all)
    
    var cate = New[int, ProductCategory]().Conv(all, 
        func(item ProductCategory) *Item[int, ProductCategory] {
            return &Item[int, ProductCategory]{
                ID: item.ID, Pid: item.Pid, Name: item.Name,
            }
        },
    )
    
    return convertToCascader(cate.Trees)
}

func convertToCascader(trees []*Item[int, ProductCategory]) []CascaderOption {
    var res []CascaderOption
    for _, node := range trees {
        res = append(res, CascaderOption{
            Value:    node.ID,
            Label:    node.Name,
            Children: convertToCascader(node.Children),
        })
    }
    return res
}

// 场景3：面包屑导航
func GetBreadcrumb(cateID int) string {
    var all []ProductCategory
    db.Find(&all)
    
    var cate = New[int, ProductCategory]().Conv(
        all, 
        func(item ProductCategory) *Item[int, ProductCategory] {
            return &Item[int, ProductCategory]{
                ID: item.ID, Pid: item.Pid, Name: item.Name,
            }
        },
    )
    
    var (
        parents = cate.FindParents(cateID)
        names []string
    )
    for _, p := range parents {
        names = append(names, p.Name)
    }
    return strings.Join(names, " > ")
}
```

### 4.3 字符串类型ID的使用

```
// 适用于 MongoDB、UUID 等场景
type Org struct {
    ID      string
    Pid     string
    Name    string
    Manager string
}

func HandleOrg() {
    var (
        orgs = []Org{
            {ID: "root", Pid: "", Name: "总公司", Manager: "张三"},
            {ID: "dept1", Pid: "root", Name: "技术部", Manager: "李四"},
            {ID: "dept2", Pid: "dept1", Name: "前端组", Manager: "王五"},
        }
        tree = New[string, Org]().Conv(
            orgs,
            func(item Org) *Item[string, Org] {
                return &Item[string, Org]{
                    ID:   item.ID,
                    Pid:  item.Pid,
                    Name: item.Name,
                    Raw:  item,
                }
            },
        )
    )
    
    // 查找前端组的所有上级
    var parents = tree.FindParents("dept2")
    for _, p := range parents {
        fmt.Printf("上级：%s（负责人：%s）\n", p.Name, p.Raw.Manager)
    }
    
    // 树转列表
    var flatList = tree.SubFlatList(tree.Trees)
    fmt.Printf("平铺后共 %d 个节点\n", len(flatList))
}
```

---

## 五、性能优化与注意事项

### 5.1 值拷贝的设计考量

```
// 为什么需要值拷贝？
var value = new(Item[T, D])
*value = *item  // 如果不拷贝，修改 Children 会影响原列表
```

如果不进行值拷贝，对 `Children` 的修改会污染原始数据，导致多次调用结果不一致。

### 5.2 递归的深度控制

对于分类树这种层级有限的场景（通常不超过10层），递归完全够用。如果担心栈溢出，可以改为迭代实现：

```
// 迭代版本的查找（防止栈溢出）
func (c *Category[T, D]) FindIterative(ID T) *Item[T, D] {
    var stack = make([]*Item[T, D], 0)
    stack = append(stack, c.Trees...)
    
    for len(stack) > 0 {
        node := stack[len(stack)-1]
        stack = stack[:len(stack)-1]
        
        if node.ID == ID {
            return node
        }
        
        if len(node.Children) > 0 {
            stack = append(stack, node.Children...)
        }
    }
    return nil
}
```

### 5.3 内存使用优化

- **复用实例**：复用 Category 实例，避免重复构建
- **按需携带 `Raw`**：如果不需要原始数据，可以不传 Raw
- **大数据量处理**：考虑分批处理或使用数据库递归查询

```
// 优化示例：不携带 Raw 数据
var res = New[int, ProductCategory]().Conv(
    data,
    func(item ProductCategory) *Item[int, ProductCategory] {
        return &Item[int, ProductCategory]{
            ID:   item.ID,
            Pid:  item.Pid,
            Name: item.Name,
            // Raw: item, // 如果不需原始数据，省略该字段
        }
    },
)
```

### 5.4 并发安全

当前实现非并发安全，如果需要在并发环境使用：

```
type SafeCategory[T keyType, D any] struct {
    mu sync.RWMutex
    *Category[T, D]
}

func (s *SafeCategory[T, D]) Conv(list []D, call func(D) *Item[T, D]) *SafeCategory[T, D] {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.Category.Conv(list, call)
    return s
}

// 其他方法类似...
```

### 5.5 常见陷阱

- **循环引用**：确保数据中没有循环引用（A的父是B，B的父是A）
- **ID唯一性**：同一棵树中ID必须唯一
- **根节点约定**：根节点的Pid必须是0或空字符串
- **空值处理**：Conv时会自动过滤无效ID

---

## 六、总结

已实现的功能

- 列表 <-----> 树的双向转换
- 查找子树（包含自身）
- 查找祖级路径
- 查找任意节点
- 泛型支持 int/string ID
- 任意业务数据类型
- 零依赖，纯标准库实现

设计心得

- 泛型：以前需要用 interface{} + 类型断言的地方，使用泛型既安全又优雅
- 值拷贝要谨慎：该拷贝时一定要拷贝，不该拷贝时不要浪费内存
- API 要直观：方法命名要符合直觉（Find、Conv、SubFlatList）

## 源码

```go
package categories

type (
	keyType interface {
		int | string
	}

	Item[T keyType, D any] struct {
		ID       T             `json:"id"`
		Pid      T             `json:"pid"`
		Name     string        `json:"name"`
		Raw      D             `json:"raw,omitempty,optional"`
		Children []*Item[T, D] `json:"children,omitempty,optional"`
	}

	Category[T keyType, D any] struct {
		List, Trees []*Item[T, D]
	}
)

func New[T keyType, D any]() *Category[T, D] {
	return &Category[T, D]{}
}

// Conv 转换列表为分类列表
func (c *Category[T, D]) Conv(list []D, call func(D) *Item[T, D]) *Category[T, D] {
	var length = len(list)
	c.List = make([]*Item[T, D], length)
	for key, item := range list {
		var v = call(item)
		if any(v.ID) == nil {
			continue
		}

		if tmp, ok := any(v.ID).(string); ok {
			if tmp == "" {
				continue
			}
		}

		if tmp, ok := any(v.ID).(int); ok {
			if tmp == 0 {
				continue
			}
		}

		c.List[key] = v
	}

	if len(c.List) <= 0 {
		return c
	}

	// 将分类结构化
	c.Trees = c.makeTrees(T(0))
	return c
}

// SubFlatList 结构化分类
func (c *Category[T, D]) makeTrees(pid T) []*Item[T, D] {
	var children []*Item[T, D]
	for _, item := range c.List {
		var value = new(Item[T, D])
		*value = *item
		if value.Pid == pid {
			children = append(children, value)
			value.Children = c.makeTrees(value.ID)
		}
	}

	if len(children) <= 0 {
		children = []*Item[T, D]{}
	}

	return children
}

// SubFlatList 子集树状结构转平铺列表
func (c *Category[T, D]) SubFlatList(trees []*Item[T, D]) []*Item[T, D] {
	var list []*Item[T, D]
	for _, item := range trees {
		var val = new(Item[T, D])
		*val = *item
		val.Children = nil

		list = append(list, val)
		if len(item.Children) > 0 {
			list = append(list, c.SubFlatList(item.Children)...)
		}
	}

	return list
}

// FindTrees 查找指定id下所有子集包含自身
func (c *Category[T, D]) FindTrees(parentIds []T, list []D, call func(D) *Item[T, D]) []T {
	var trees = c.Conv(list, call).Trees
	if len(trees) <= 0 {
		return nil
	}

	var (
		ids     = append([]T{}, parentIds...)
		records []*Item[T, D]
	)
	// 查询
	for _, id := range parentIds {
		var item = c.FindId(id, trees)
		if item == nil {
			continue
		}

		records = append(records, item)
	}

	for _, item := range records {
		for _, item := range c.SubFlatList(item.Children) {
			ids = append(ids, item.ID)
		}
	}

	return ids
}

func (c *Category[T, D]) FindId(id T, data []*Item[T, D]) *Item[T, D] {
	for _, item := range data {
		if item.ID == id {
			return item
		}

		if len(item.Children) > 0 {
			var data = c.FindId(id, item.Children)
			if data != nil {
				return data
			}
		}
	}

	return nil
}

// Find 查找id
func (c *Category[T, D]) Find(ID T) *Item[T, D] {
	return c.find(ID, c.Trees)
}

func (c *Category[T, D]) find(ID T, subs []*Item[T, D]) *Item[T, D] {
	for _, item := range subs {
		if item.ID == ID {
			return item
		}

		if len(item.Children) > 0 {
			if val := c.find(ID, item.Children); val != nil {
				return val
			}
		}
	}

	return nil
}

// FindParents 查找祖级
func (c *Category[T, D]) FindParents(ID T) []*Item[T, D] {
	var current *Item[T, D]
	for _, item := range c.List {
		if item.ID == ID {
			current = new(Item[T, D])
			*current = *item
		}
	}

	if current == nil {
		return nil
	}

	var data = []*Item[T, D]{current}
	if pid, ok := any(current.Pid).(string); ok {
		if pid == "" {
			return data
		}
	}

	if pid, ok := any(current.Pid).(int); ok {
		if pid == 0 {
			return data
		}
	}

	var (
		list  = append([]*Item[T, D]{current}, c.findParents(current.Pid)...)
		res   = make([]*Item[T, D], len(list))
		index = 0
	)
	for i := len(list) - 1; i >= 0; i-- {
		res[index] = list[i]
		index += 1
	}

	return res
}

func (c *Category[T, D]) findParents(pid T) []*Item[T, D] {
	var parents []*Item[T, D]
	for _, item := range c.List {
		if item.ID == pid {
			var val = new(Item[T, D])
			*val = *item
			parents = append(parents, val)

			if pid, ok := any(val.Pid).(string); ok {
				if pid == "" {
					break
				}
			}

			if pid, ok := any(val.Pid).(int); ok {
				if pid == 0 {
					break
				}
			}

			parents = append(parents, c.findParents(val.Pid)...)
		}
	}

	return parents
}

```