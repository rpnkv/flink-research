package org.apache.flink.walkthrough.windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{FromIteratorFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource
import org.apache.flink.walkthrough.util.{Event, EventSource}
import org.apache.flink.walkthrough.windows.SingleEventSink.logger
import org.slf4j.LoggerFactory

import java.io.Serializable
import java.util
import java.util.{Calendar, Iterator}

object WindowPracticeJob {

  private val logger = LoggerFactory.getLogger(WindowPracticeJob.getClass)

  def main(args: Array[String]): Unit = {
    logger.warn("started")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val transactions: DataStream[Event] = env
      .addSource(EventSource.apply(0,0))
      .name("transactions")

    val window: AllWindowedStream[List[Event], TimeWindow] = transactions
      .map(event => List(event))
      .windowAll(SlidingEventTimeWindows.of(Time.milliseconds(2000), ProducingDelay.WINDOW_DELAY))
      //.windowAll(SlidingProcessingTimeWindows.of(ProducingDelay.WINDOW_DELAY, Time.milliseconds(2000)))
      //.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(ProducingDelay.WINDOW_DELAY)))
      //.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(ProducingDelay.WINDOW_DELAY)))

    window
      .reduce((list1, list2) => list1 ::: list2)
      .addSink(new EventCollectionSink)

    env.execute("windows")
    logger.warn("completed")
  }
}

object SingleEventSink{
  private val logger = LoggerFactory.getLogger(SingleEventSink.getClass)
}

class SingleEventSink extends SinkFunction[Event] {
  override def invoke(value: Event, context: SinkFunction.Context): Unit = {
    logger.info(value.toString)
  }
}


/*object EventCollectionSink{
  private val lg = LoggerFactory.getLogger(EventCollectionSink.getClass)
}*/

class EventCollectionSink extends SinkFunction[List[Event]] {
  override def invoke(value: List[Event], context: SinkFunction.Context): Unit = {
    LoggerFactory.getLogger(this.getClass).info(s"list of ${value.length} elements ${value.mkString(" ")}")
    println(s"list of ${value.length} elements ${value.mkString(" ")}")
  }
}

object ProducingDelay {
  val EVENTS_IN_WINDOW: Int = 20
  val PRODUCING_DELAY: Int = 200
  val WINDOW_DELAY: Time = Time.milliseconds(PRODUCING_DELAY * EVENTS_IN_WINDOW)
}