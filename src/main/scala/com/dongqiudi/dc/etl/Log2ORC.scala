package com.dongqiudi.dc.etl

import java.util.Date

import com.dongqiudi.dc.util.{FileUtil, ORCConfigUtil, ReflectUtil}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * 通用的日志处理类, 用于 ETL 过程中由原始文本格式转换为 ORC 格式
  * Created by matrix on 17/9/27.
  */
object Log2ORC extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    val sparkContext = new SparkContext(sparkConf)

    val appName = sparkContext.getConf.get("spark.app.name")
    val inputPath = sparkContext.getConf.get("spark.app.inputPath")
    val outputPath = sparkContext.getConf.get("spark.app.outputPath")
    val coalesceSize = sparkContext.getConf.get("spark.app.coalesce.size").toInt

    sparkContext.hadoopConfiguration.set("dfs.socket.timeout", "180000")
    val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

    val loadTime = appName.substring(appName.lastIndexOf("_") + 1)
    val hiveTable = appName.substring(appName.indexOf("_") + 1, appName.lastIndexOf("_"))
    val logModel = getLogModel(hiveTable, hiveTable.length, 0, 0, "")

    var partitions = "day,hour"
    if (logModel.contains("Proxy")) {
      partitions = "day,hour,domain"
    }

    logInfo("[" + appName + "] inputPath: " + inputPath)
    logInfo("[" + appName + "] outputPath: " + outputPath)
    logInfo("[" + appName + "] logModel: " + logModel)
    logInfo("[" + appName + "] loadTime: " + loadTime)
    logInfo("[" + appName + "] coalesceSize: " + coalesceSize)

    try {
      // 参数校验
      val array = hiveTable.split("\\.")
      if (array.length != 2) {
        logError("[" + appName + "] Error: hive table should like this db.table and value is " + hiveTable)
        System.exit(1)
      }
      val hiveTablePath = array(0) + ".db/" + array(1)


      // 动态获取日志模型对象
      ReflectUtil.reflect(logModel)
      if (ReflectUtil.struct == null) {
        logError("[" + appName + "] Error: Instance LogModel Failed!")
        System.exit(1)
      }

      // 设置 ORC 参数
      ORCConfigUtil.configSettings(sparkContext)

      val jobContext = new HiveContext(sparkContext)

      // 计算 coalesce 大小
      val coalesce = FileUtil.makeCoalesce(fileSystem, inputPath, coalesceSize)
      logInfo("[" + appName + "] coalesce: " + coalesce)

      var jobDF = jobContext.read.text(inputPath.split(","): _*).coalesce(coalesce)

      // 生成 ORC 文件
      if (logModel.equals("EventsNginxSensor")) {
        jobDF = jobContext.createDataFrame(jobDF.flatMap(x => ReflectUtil.parseLog2Array(logModel, x.getString(0))).filter(_.length != 1), ReflectUtil.struct)
      } else {
        jobDF = jobContext.createDataFrame(jobDF.map(x => ReflectUtil.parseLog(logModel, x.getString(0))).filter(_.length != 1), ReflectUtil.struct)
      }
      jobDF.write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath)

      // 转储结果文件
      val begin = new Date().getTime
      FileUtil.moveORCFiles(fileSystem, outputPath, appName, hiveTablePath, partitions)
      logInfo("[" + appName + "] move ORC File: " + (new Date().getTime - begin) + " ms")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError("[" + appName + "] Error: " + e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }

  /**
    * 根据 Hive 表名获取日志模型的类名
    * business.logic_ap => BusinessLogicAP
    * business.logic_dab => BusinessLogicDAB
    * dongqiudi.nginx_analyse = > DongqiudiNginxAnalyse
    * @param hiveTable 表名
    * @param len 表名长度
    * @param i 索引
    * @param flag 标记特殊字符(. | _)
    * @param result 类名
    * @return
    */
  def getLogModel(hiveTable: String, len: Int, i: Int, flag: Int, result: String): String = {
    if (i == 0) getLogModel(hiveTable, len, i + 1, 0, result + hiveTable.charAt(i).toUpper) // 首字母大写
    else if (i == len) result
    else if (hiveTable.charAt(i) == '.') getLogModel(hiveTable, len, i + 1, 1, result) // 跳过特殊字符并设置 flag 值
    else if (hiveTable.charAt(i) == '_') getLogModel(hiveTable, len, i + 1, 2, result)
    else if (flag == 1) getLogModel(hiveTable, len, i + 1, 0, result + hiveTable.charAt(i).toUpper) // 特殊字符后面的第一个字面大写
    else if (flag == 2) {
      // 如果 _ 字符后面少于四个字符则全部大写, 否则只有首字母大写
      if (len - i < 4) {
        getLogModel(hiveTable, len, i + 1, 3, result + hiveTable.charAt(i).toUpper)
      } else {
        getLogModel(hiveTable, len, i + 1, 0, result + hiveTable.charAt(i).toUpper)
      }
    }
    else if (flag == 3) getLogModel(hiveTable, len, i + 1, 3, result + hiveTable.charAt(i).toUpper)
    else getLogModel(hiveTable, len, i + 1, 0, result + hiveTable.charAt(i))
  }
}
