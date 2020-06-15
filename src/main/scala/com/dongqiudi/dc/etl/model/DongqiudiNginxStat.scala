package com.dongqiudi.dc.etl.model

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * 懂球帝-打点日志
  * Created by matrix on 17/11/9.
  */
object DongqiudiNginxStat extends Serializable with Logging {
  val struct: StructType = DongqiudiNginxAPI.struct

  def parseLog(line: String): Row = {
    DongqiudiNginxAPI.parseLog(line)
  }
}
