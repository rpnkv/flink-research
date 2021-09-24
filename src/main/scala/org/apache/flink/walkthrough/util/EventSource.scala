package org.apache.flink.walkthrough.util

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.walkthrough.windows.ProducingDelay

class EventSource(range: Seq[Int], producingDelay: Int) extends SourceFunction[Event] {

  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    range
      .foreach(
        value => {
          ctx.collect(Event(value, System.currentTimeMillis()))
          //ctx.collectWithTimestamp(Event(value), System.currentTimeMillis())
          Thread.sleep(ProducingDelay.PRODUCING_DELAY)
        }
      )
  }

  override def cancel(): Unit = {}
}

object EventSource {
  def apply(eventsCount: Int, producingDelay: Int): EventSource = {
    val range = Range.Int.apply(1, eventsCount + 1, 1)
    new EventSource(range, producingDelay)
  }
}
