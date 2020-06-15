package com.dongqiudi.dc.etl.model

import com.dongqiudi.dc.util.LogConvertUtil
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.beans.BeanProperty

/**
  * 广告-AP服务-业务日志
  * Created by matrix on 17/9/27.
  */
object BusinessLogicAP extends Serializable with Logging {

  // 日志格式示例
  /*
  {u'@timestamp': u'2017-09-26T20:59:59.977Z',
    u'@version': u'1',
    u'adtype': [u'2', u'7', u'9'],
    u'apvc': u'5.7.1',
    u'cost_type': 4,
    u'creativeID': 107127,
    u'ct': u'article_2273,article_14572',
    u'day': u'2017-09-27',
    u'deploy': u'business_adserver',
    u'file': u'/var/lib/jenkins/workspace/prod-ad-server/src/adserver/service/newservice.go',
    u'filename': u'adserver.log',
    u'geo': u'1156310000',
    u'host': u'adserver-3',
    u'hour': u'04',
    u'level': u'info',
    u'line': 271,
    u'msg': u'get ad to show for user...',
    u'net': u'wifi',
    u'os': u'ios',
    u'path': u'/data/apps/adserver/logs/adserver.log',
    u'pgID': u'1.3.1',
    u'platform': 1,
    u'posID': u'100000046',
    u'rid': u'56a74b94f75e64884cb197b1864f08af',
    u'time': u'2017-09-27T04:59:59+08:00',
    u'timestamp': u'2017-09-27T04:59:59+08:00',
    u'type': u'adserver',
    u'unitID': 105644,
    u'usertag': u''}
  */

  val struct = StructType(Array(
    StructField("os", StringType),
    StructField("ct", StringType),
    StructField("rid", StringType),
    StructField("geo", StringType),
    StructField("net", StringType),

    StructField("apvc", StringType),
    StructField("host", StringType),
    StructField("pg_id", StringType),
    StructField("pos_id", StringType),
    StructField("unit_id", StringType),

    StructField("ad_type", ArrayType(StringType)),
    StructField("platform", StringType),
    StructField("user_tag", StringType),
    StructField("cost_type", StringType),
    StructField("creative_id", StringType),

    StructField("log_timestamp", StringType),


    StructField("day", StringType),
    StructField("hour", StringType)
  ))

  @JsonIgnoreProperties(ignoreUnknown = true)
  class BusinessLogicAPLogModel {
    @JsonProperty("os") @BeanProperty var os = ""
    @JsonProperty("ct") @BeanProperty var ct = ""
    @JsonProperty("rid") @BeanProperty var rid = ""
    @JsonProperty("geo") @BeanProperty var geo = ""
    @JsonProperty("net") @BeanProperty var net = ""
    @JsonProperty("apvc") @BeanProperty var apvc = ""
    @JsonProperty("host") @BeanProperty var host = ""
    @JsonProperty("pgID") @BeanProperty var pgID = ""
    @JsonProperty("posID") @BeanProperty var posID = ""
    @JsonProperty("unitID") @BeanProperty var unitID = ""
    @JsonProperty("adtype") @BeanProperty var adType: Array[String] = Array[String]()
    @JsonProperty("usertag") @BeanProperty var userTag = ""
    @JsonProperty("platform") @BeanProperty var platform = ""
    @JsonProperty("cost_type") @BeanProperty var costType = ""
    @JsonProperty("@timestamp") @BeanProperty var timestamp = ""
    @JsonProperty("creativeID") @BeanProperty var creativeID = ""
  }

  def parseLog(line: String): Row = {
    try {
      // JSON to POJO
      val newLine = line.replaceAll("\\\\(?![/u\"])", "\\\\\\\\")
      val logModel = LogConvertUtil.mapper.readValue(newLine, classOf[BusinessLogicAPLogModel])

      // timestamp
      val timeMSG = LogConvertUtil.getTimeMSG(logModel.getTimestamp)
      val timestamp = timeMSG(0)
      val day = timeMSG(1)
      val hour = timeMSG(2)

      Row (
        logModel.getOs,
        logModel.getCt,
        logModel.getRid,
        logModel.getGeo,
        logModel.getNet,

        logModel.getApvc,
        logModel.getHost,
        logModel.getPgID,
        logModel.getPosID,
        logModel.getUnitID,

        logModel.getAdType,
        if (logModel.getUserTag.length == 0) "-" else logModel.getUserTag,
        logModel.getPlatform,
        logModel.getCostType,
        logModel.getCreativeID,

        timestamp,

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
