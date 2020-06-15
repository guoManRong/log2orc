package com.dongqiudi.dc.etl.model

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * 懂球帝-实时打点日志
  * Created by andone1cc 30/06/2018.
  */
object DongqiudiNginxNewstat extends Serializable with Logging {
  val struct: StructType = DongqiudiNginxAPI.struct

  def parseLog(line: String): Row = {
    DongqiudiNginxAPI.parseLog(line)
  }
}
