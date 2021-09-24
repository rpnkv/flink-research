package org.apache.flink.walkthrough.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

trait FlinkEnvironment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

}
