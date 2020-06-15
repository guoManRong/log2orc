package com.dongqiudi.dc.etl.model

import com.dongqiudi.dc.util.LogConvertUtil
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.beans.BeanProperty

/**
  * 广告-DAB服务-除点击、暴光之外的日志
  * Created by matrix on 17/11/13.
  */
object BusinessLogicDabo extends Serializable with Logging {
  val struct = StructType(Array(
    StructField("rpc", StringType),
    StructField("line", StringType),
    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class BusinessLogicDaboLogModel {
    @JsonProperty("@timestamp")
    @BeanProperty var timestamp = ""
    @JsonProperty("rpc")
    @BeanProperty var rpc = ""
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      val newLine = line.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[BusinessLogicDaboLogModel])

      // timestamp
      val timeMSG = LogConvertUtil.getTimeMSG(logModel.getTimestamp)
      val day = timeMSG(1)
      val hour = timeMSG(2)

      Row(
        logModel.rpc,
        line,
        day,
        hour
      )
    } catch {
      case e: Exception =>
        logError("ParseError log[" + line + "] msg[" + e.getMessage + "]")
        Row(0)
    }
  }
}
