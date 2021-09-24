package org.apache.flink.walkthrough.parallelism

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.walkthrough.util.{Event, EventSource, FlinkEnvironment, StdoutSink}

object MaxForPeriodSandbox extends App with FlinkEnvironment {

  val source: DataStream[Event] = env
    .addSource(EventSource.apply(80, 150))
    .assignAscendingTimestamps(_.timestamp)

  val groupedByKey = source
    .keyBy(value => value.payload / 10)


  val maxValues = groupedByKey
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(300)))
    .max("payload")

  maxValues.addSink(new StdoutSink)

  env.execute()

}
