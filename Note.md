DataSet 和 DataStream 可以认识是一个不变的集合。经过算子转换后的 dataset 和datastream 都是新的。

DataSet 是一个有界的数据

DataStream是无界的数据 

批处理 会创建一个dataset 

实时流处理会创建一个datastream





# 大数据的处理流程
大致都是一样的
MR： input -> map / reduce -> output
storm： input -> spout/bolt -> output
Spark：input -> transformation/action -> ouput
Flink：input -> transformation/sink -> output

# Flink的编程模型

flink的编程过程是一个很有规律的转换过程。每个程序都包含相同的基本部分：
1. 获取一个可以执行的环境
2. 加载、创建初始化数据
3. 数据的转换   transformation
4. 指定结果的输出位置 sink 
5. 触发程序的执行

# Flink延迟执行

flink所有的程序都是延迟执行的，在程序运行的时候，数据的加载和转换并不会立即执行，而是将你的任务加入到执行计划里面去。只有当你调用execute方法去触发的时候，才会执行。

# Flink transformation中的Key

flink 的数据不是基于kv结构的，所以我们不用将数据包装成k,v 数据结构。而是在聚合分组的时候，将一个函数作用在上面来确定用什么来当k来聚合操作。

## 定义key的方式

### tuple来定义key

tuple就是元组的意思，就是一组数据。flink可以使用groupy 或者 keyby 然后使用tuple的索引位置来确定key。

我们可以指定多个为key，如(A,B,C) 可以groupby(0,1) 就是以(A,B)为key

针对嵌套tuple，如 ((A,B),C) 那么 groupby(0) 就是以(A,B) 为key的。如果你想要指定到内置tuple里面的元素来做key，就要使用Field Expression了。

### Field Expression来定义key

这个规则还挺多 见官网 https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/api_concepts.html#define-keys-using-field-expressions



# Flink 支持的数据类型

什么叫tuple？就是固定数量，可以有各种类型的一种数组结构

重点类型 1，2，3，6



# Dataset api 开发

## datasource

多种获取数据源的方式，可以从文件，csv，压缩文件等地方获取dataset。都是通过env的方法来获取的。

## transformation

转换/算子。

## sink


# DataStream api 开发

## source
有几个大类
- 基于文件的（stream很少有该使用场景）
- 基于socket的
- 基于集合的
- 自定义的
    - 比如从kafka里面去读取
    
### 自定义source
需要使用addSource的方式添加进去。
addSource(SourceFunction<OUT> function)

自定义数据源有三种方式
1. 实现 SourceFunction 针对不可以并行的sources
2. 实现 ParallelSourceFunction 接口
3. 继承 RichParallelSourceFunction 对要并行的数据源


























