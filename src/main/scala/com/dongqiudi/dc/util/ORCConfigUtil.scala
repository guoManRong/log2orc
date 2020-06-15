package com.dongqiudi.dc.util

import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import org.apache.hadoop.hive.ql.io.orc.OrcFile.{CompressionStrategy, EncodingStrategy}
import org.apache.spark.SparkContext

/**
  * 封装 ORC 配置
  * Created by matrix on 17/8/29.
  */
object ORCConfigUtil {
  /**
    * 构建分区路径规则
    * @return
    */
  def getTemplate(partitions: String): String = {
    var template = ""
    val partitionArray = partitions.split(",")
    for (i <- 0 until partitionArray.length) {
      template = template + "/" + partitionArray(i) + "=*"
    }
    template
  }

  def configSettings(sparkContext: SparkContext) {
    sparkContext.hadoopConfiguration.set("orc.compress", CompressionKind.ZLIB.name())
    sparkContext.hadoopConfiguration.set("orc.stripe.size", (64 * 1024 * 1024).toString)
    sparkContext.hadoopConfiguration.set("orc.block.size", (128 * 1024 * 1024).toString)
    sparkContext.hadoopConfiguration.set("orc.encoding.strategy", EncodingStrategy.SPEED.name())
    sparkContext.hadoopConfiguration.set("orc.compress.size", (256 * 1024).toString)

    sparkContext.hadoopConfiguration.set("orc.row.index.stride", "10000")
    sparkContext.hadoopConfiguration.set("orc.create.index", "true")
    sparkContext.hadoopConfiguration.set("hive.exec.orc.memory.pool", "0.5")
    sparkContext.hadoopConfiguration.set("hive.exec.orc.compression.strategy", CompressionStrategy.SPEED.name())
    sparkContext.hadoopConfiguration.set("hive.exec.orc.default.buffer.size", (256 * 1024).toString)
    sparkContext.hadoopConfiguration.set("hive.exec.orc.dictionary.key.size.threshold", "0.8")
  }
}