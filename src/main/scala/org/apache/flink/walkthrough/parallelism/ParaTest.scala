package org.apache.flink.walkthrough.parallelism

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.util.{Event, EventSource, FlinkEnvironment}

object ParaTest extends App with FlinkEnvironment {

  val stream = env
    .addSource(EventSource.apply(50, 10))

  val grouped: KeyedStream[Event, Int] = stream.keyBy(event => event.payload % 10)

  grouped.process(new KeyedProcessFunction[Int, Event, List[Event]] {
    override def processElement(value: Event, ctx: KeyedProcessFunction[Int, Event, List[Event]]#Context, out: Collector[List[Event]]): Unit = {

    }
  }
  )


}

//class KeyedEventsProcessor extends KeyedProcessFunction
