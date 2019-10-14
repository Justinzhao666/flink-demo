package top.zhaohaoren.flink.ch05.scala

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class DataStreamApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    socketFunction(env)
    env.execute("DataStreamApp")
  }

  def socketFunction(env: StreamExecutionEnvironment): Unit = {
    val source = env.socketTextStream("localhost", 9999)
    source.print().setParallelism(1)
  }
}
