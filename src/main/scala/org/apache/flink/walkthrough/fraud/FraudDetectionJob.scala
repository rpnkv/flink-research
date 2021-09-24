package org.apache.flink.walkthrough.fraud


import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.source.TransactionSource
import org.apache.flink.walkthrough.util.FlinkEnvironment
import org.slf4j.LoggerFactory

/**
 * Skeleton code for the DataStream code walkthrough
 */
object FraudDetectionJob extends FlinkEnvironment{

  private val logger = LoggerFactory.getLogger(FraudDetectionJob.getClass)

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val transactions: DataStream[Transaction] = env
      .addSource(new TransactionSource)
      .name("transactions")

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")


    alerts
      .addSink(new AlertSink)
      .name("send-alerts")

    env.execute("Fraud Detection")
  }
}
