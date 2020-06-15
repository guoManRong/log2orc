package com.dongqiudi.dc.etl.model

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  *
  * Created by matrix on 18/2/28.
  */
object DongqiudiNginxNode extends Serializable with Logging {
  val struct: StructType = DongqiudiNginxAPI.struct

  def parseLog(line: String): Row = {
    DongqiudiNginxAPI.parseLog(line)
  }
}
