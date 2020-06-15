package com.dongqiudi.dc

import java.util.Date
import java.util.concurrent.ConcurrentSkipListMap

import com.dongqiudi.dc.etl.Log2ORC.logInfo
import com.dongqiudi.dc.util.{CryptoUtils, FileUtil, ORCConfigUtil}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  *
  * Created by matrix on 17/11/6.
  */
object ORC2ORC4ADNginxDAB extends Logging {

  val nginxDABStruct = StructType(Array(
    StructField("plat_form", StringType),
    StructField("channel", StringType),
    StructField("tab", StringType),
    StructField("pos_id", StringType),
    StructField("execplan_id", StringType),
    StructField("unit_id", StringType),
    StructField("creative_id", StringType),
    StructField("cost_type", StringType),
    StructField("delivery_mode", StringType),
    StructField("agent_id", StringType),
    StructField("customer_id", StringType),
    StructField("geo", StringType),
    StructField("cost", StringType),
    StructField("content", StringType),
    StructField("audience", StringType),
    StructField("deploy", StringType),
    StructField("ex2", StringType),
    StructField("pgid", StringType),
    StructField("qt", StringType),
    StructField("rid", StringType),
    StructField("position", StringType),
    StructField("cp", StringType),
    StructField("apvc", StringType),
    StructField("posid", StringType),

    StructField("uuid", StringType),
    StructField("lang", StringType),
    StructField("platform", StringType),
    StructField("version", StringType),
    StructField("country", StringType),

    StructField("province", StringType),
    StructField("log_timestamp", StringType),
    StructField("server_addr", StringType),
    StructField("remote_addr", StringType),
    StructField("xff", StringType),

    StructField("scheme", StringType),
    StructField("host", StringType),
    StructField("method", StringType),
    StructField("uri", StringType),
    StructField("url", StringType),

    StructField("protocol", StringType),
    StructField("status", StringType),
    StructField("size", StringType),
    StructField("request_time", StringType),
    StructField("upstream_time", StringType),

    StructField("upstream_host", StringType),
    StructField("referer", StringType),
    StructField("agent", StringType),
    StructField("token", StringType),

    StructField("request_id", StringType),
    StructField("ip_area_iso", StringType),
    StructField("ip_area", StringType),
    StructField("day", StringType),
    StructField("hour", StringType)
  ))
  def main(args: Array[String]) {
    val sparkConf = new SparkConf
    val sparkContext = new SparkContext(sparkConf)

    val appName = sparkContext.getConf.get("spark.app.name")

    val inputPath = sparkContext.getConf.get("spark.app.inputPath")
    val outputPath = sparkContext.getConf.get("spark.app.outputPath")

    sparkContext.hadoopConfiguration.set("dfs.socket.timeout", "180000")
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

    logInfo("[" + appName + "] inputPath: " + inputPath)
    logInfo("[" + appName + "] outputPath: " + outputPath)

    val partitions = "day,hour"

    try {

      val jobContext = new HiveContext(sparkContext)
      //val coalesce = FileUtil.makeCoalesce(fileSystem, inputPath, coalesceSize)
      val jobDF = jobContext.read.format("orc").load(inputPath)

      // val columns = "uuid, lang, platform, version, country, province, log_timestamp, server_addr, remote_addr, xff, scheme, host, method, pos, cid, dapid, imp_url, uri, url, protocol, status, size, request_time, upstream_time, upstream_host, referer, agent, access_token, request_id, ip_area_iso, ip_area, day, hour"
      val columns = "_col0, _col1, _col2, _col3, _col4, _col5, _col6, _col7, _col8, _col9, _col10, _col11, _col12, _col13, _col14, _col15, _col16, _col17, _col18, _col19, _col20, _col21, _col22, _col23, _col24, _col25, _col26, _col27, day, hour"

      jobContext.createDataFrame(jobDF.select(columns.split(", ").map(x => jobDF.col(x)): _*).map(x => {
        var plat_form = "-"
        var channel = "-"
        var tab = "-"
        var pos_id = "-"
        var execplan_id = "-"
        var unit_id = "-"
        var creative_id = "-"
        var cost_type = "-"
        var delivery_mode = "-"
        var agent_id = "-"
        var customer_id = "-"
        var geo = "-"
        var cost = "-"
        var content = "-"
        var audience = "-"
        var deploy = "-"
        var ex2 = "-"
        var pgid = "-"
        var qt = "-"
        var rid = "-"
        var position = "-"
        var cp = "-"
        var apvc = "-"
        var posid = "-"

        var uuid = x.getString(0)
        var lang = x.getString(1)
        var platform = x.getString(2)
        var version = x.getString(3)
        var country = x.getString(4)
        var province = x.getString(5)
        var log_timestamp = x.getString(6)
        var server_addr = x.getString(7)
        var remote_addr = x.getString(8)
        var xff = x.getString(9)
        var scheme = x.getString(10)
        var host = x.getString(11)
        var method = x.getString(12)
        var pos = x.getString(13)
        var cid = x.getString(14)
        var dapid = x.getString(15)
        var imp_url = x.getString(16)
        var uri = x.getString(17)
        var url = x.getString(18)
        var protocol = x.getString(19)
        var status = x.getString(20)
        var size = x.getString(21)
        var request_time = x.getString(22)
        var upstream_time = x.getString(23)
        var upstream_host = x.getString(24)
        var referer = x.getString(25)
        var agent = x.getString(26)
        var token = x.getString(27)

        var request_id = "-"
        var ip_area_iso = "-"
        var ip_area = "-"
        var day = 0
        var hour = 0
        if (x.length == 33) {
          request_id = x.getString(28)
          ip_area_iso = x.getString(29)
          ip_area = x.getString(30)

          day = x.getInt(31)
          hour = x.getInt(32)
        } else {
          day = x.getInt(28)
          hour = x.getInt(29)
        }

        var hours = hour.toString
        if (hour < 10) {
          hours = "0" + hour
        }


        if (url.endsWith("v3") && (url.startsWith("/imp/") || url.startsWith("/click/"))) {
          val uriParams = initURIParams(uri)

          if (uriParams.get("ex2") != null) {
            ex2 = uriParams.get("ex2")
          }

          if (uriParams.get("pgid") != null) {
            pgid = uriParams.get("pgid")
          }

          if (uriParams.get("qt") != null) {
            qt = uriParams.get("qt")
          }

          if (uriParams.get("position") != null) {
            position = uriParams.get("position")
          }

          if (uriParams.get("cp") != null) {
            cp = uriParams.get("cp")
          }

          if (uriParams.get("apvc") != null) {
            apvc = uriParams.get("apvc")
          }

          if (uriParams.get("posid") != null) {
            posid = uriParams.get("posid")
          }

          if (url.startsWith("/imp/")) {
            deploy = "imp"
          } else if (url.startsWith("/click/")) {
            deploy = "click"
          }

          if (uriParams.get("ex") != null && uriParams.get("ex") != "-") {
            var ex = uriParams.get("ex")
            ex = ex.replaceAll("_", "=")
            ex = ex.replaceAll(" ", "+")
            ex = ex.replaceAll("--G--", "&")
            ex = ex.replaceAll("--F--", "#")
            ex = ex.replaceAll("--E--", "%")
            ex = ex.replaceAll("--D--", "=")
            ex = ex.replaceAll("--C--", "?")
            ex = ex.replaceAll("--B--", "/")
            ex = ex.replaceAll("--A--", "+")
            val value = CryptoUtils.decrypt(ex)
            val array = value.split(",", -1)
            if (array.length == 15) {
              plat_form = array(0)
              channel = array(1)
              tab = array(2)
              pos_id = array(3)
              execplan_id = array(4)
              unit_id = array(5)
              creative_id = array(6)
              cost_type = array(7)
              delivery_mode = array(8)
              agent_id = array(9)
              customer_id = array(10)
              geo = array(11)
              cost = array(12)
              content = array(13)
              audience = array(14)
            }
          }
        }

        Row(
          plat_form,
          channel,
          tab,
          pos_id,
          execplan_id,
          unit_id,
          creative_id,
          cost_type,
          delivery_mode,
          agent_id,
          customer_id,
          geo,
          cost,
          content,
          audience,
          deploy,
          ex2,
          pgid,
          qt,
          rid,
          position,
          cp,
          apvc,
          posid,
          uuid,
          lang,
          platform,
          version,
          country,
          province,
          log_timestamp,
          server_addr,
          remote_addr,
          xff,
          scheme,
          host,
          method,
          uri,
          url,
          protocol,
          status,
          size,
          request_time,
          upstream_time,
          upstream_host,
          referer,
          agent,
          token,
          request_id,
          ip_area_iso,
          ip_area,
          day.toString,
          hours
        )
      }).filter(_.length != 1), nginxDABStruct)
        .write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath)

      // 转储结果文件
      val begin = new Date().getTime
      val hiveTablePath = "business.db/nginx_dab"
      FileUtil.moveORCFiles(fileSystem, outputPath, appName, hiveTablePath, partitions)
      logInfo("[" + appName + "] move ORC File: " + (new Date().getTime - begin) + " ms")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }

  def initURIParams(uri: String): ConcurrentSkipListMap[String, String] = {
    val uriParams = new ConcurrentSkipListMap[String, String]
    var array = uri.split("\\?")
    if (array.length == 1) uriParams
    val params = array(1)
    array = params.split("&")
    for (item <- array) {
      if (item.contains("=")) {
        val idx = item.indexOf("=")
        val key = item.substring(0, idx)
        var value = item.substring(idx + 1)
        if (value.length == 0) value = "-"
        uriParams.put(key, value)
      }
    }
    uriParams
  }

}
