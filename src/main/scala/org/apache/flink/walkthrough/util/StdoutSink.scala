package org.apache.flink.walkthrough.util

import org.apache.flink.streaming.api.functions.sink.SinkFunction

class StdoutSink extends SinkFunction[Event]{
  override def invoke(value: Event, context: SinkFunction.Context): Unit = {
    println(value)
  }
}
