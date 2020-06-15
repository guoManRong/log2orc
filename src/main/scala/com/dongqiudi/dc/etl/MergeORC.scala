package com.dongqiudi.dc.etl

import java.util.Date

import com.dongqiudi.dc.util.ORCConfigUtil
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * ETL 过程中会生成很多个小的 ORC 文件
  * 虽然可以提高 ETL 的处理速度, 但过多的小文件会对 NameNode 造成压力
  * 因此需要对小文件进行合并
  * Created by matrix on 17/12/26.
  */
object MergeORC extends Logging {
  val hivePath = "/user/hive/warehouse/"
  var partitions = "day,hour"
  var outputPath = ""
  var fileSystem: FileSystem = _
  var jobContext: HiveContext = _
  var appName = ""
  var hiveTablePath = ""


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.scheduler.mode", "FAIR")

    val sparkContext = new SparkContext(sparkConf)

    appName = sparkContext.getConf.get("spark.app.name")
    val hiveTable = sparkContext.getConf.get("spark.app.hiveTable")
    outputPath = sparkContext.getConf.get("spark.app.outputPath")
    val filterPartition = sparkContext.getConf.get("spark.app.filterPartition")

    sparkContext.hadoopConfiguration.set("dfs.socket.timeout", "180000")
    fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

    if (hiveTable.contains("proxy")) {
      partitions = "day,hour,domain"
    }

    logInfo("[" + appName + "] outputPath: " + outputPath)
    logInfo("[" + appName + "] filterPart: " + filterPartition)

    try {
      // 设置 ORC 参数
      ORCConfigUtil.configSettings(sparkContext)

      // 参数校验
      val array = hiveTable.split("\\.")
      if (array.length != 2) {
        logError("[" + appName + "] Error: hive table should like this db.table and value is " + hiveTable)
        System.exit(1)
      }
      hiveTablePath = array(0) + ".db/" + array(1)
      var inputPath = hivePath + hiveTablePath

      val partitionNameArray = partitions.split(",")
      val partitionFilterArray = filterPartition.split("_")
      for (i <- 0 until partitionFilterArray.length) {
        inputPath += "/" + partitionNameArray(i) + "=" + partitionFilterArray(i)
      }
      logInfo("[" + appName + "] inputPath: " + inputPath)


      jobContext = new HiveContext(sparkContext)
      val allFiles = fileSystem.listStatus(new Path(inputPath))
      process(allFiles)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError("[" + appName + "] Error: " + e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }

  def process(allFiles: Array[FileStatus]): Unit = {
    if (allFiles.exists(p => p.getPath.getName.startsWith("0"))) {
      val filePath = allFiles(0).getPath.toString

      val jobDF = jobContext.read.format("orc").load(allFiles.filter(x => x.getPath.getName.startsWith("0")).map(x => {
        x.getPath.toString
      }): _*).coalesce(1)

      jobDF.write.mode(SaveMode.Overwrite).format("orc").partitionBy(partitions.split(","): _*).save(outputPath + "/tmp")

      // 转储结果文件
      val begin = new Date().getTime
      allFiles.map(p => {
        val orcFilePath = p.getPath.toString
        val partitionPath = orcFilePath.substring(orcFilePath.indexOf(hivePath) + hivePath.length + hiveTablePath.length, orcFilePath.lastIndexOf("/"))
        val backupPath = outputPath + "/backup" + partitionPath + "/" + p.getPath.getName
        val path = new Path(backupPath)

        if (!fileSystem.exists(path.getParent)) {
          fileSystem.mkdirs(path.getParent)
        }
        logInfo("[" + appName + "] rename: " + p.getPath.toString + " => " + path.toString)
        fileSystem.rename(p.getPath, path)
      })


      var pathStr = filePath.substring(filePath.indexOf(hivePath) + hivePath.length, filePath.lastIndexOf("/"))
      pathStr = pathStr.replace(".db", "")
      for (partition <- partitions.split(",")) {
        pathStr = pathStr.replace(partition + "=", "")
      }
      pathStr = pathStr.replace("/", "-")

      val orcFiles = SparkHadoopUtil.get.globPath(new Path(outputPath + "/tmp" + ORCConfigUtil.getTemplate(partitions) + "/*.orc"))
      orcFiles.foreach(orcFile => {
        val fileFlag = orcFile.getName.split("-")(2)
        val mergeFileName = pathStr + "-" + fileFlag + ".orc"
        val mergeFilePath = new Path(filePath.substring(0, filePath.lastIndexOf("/")) + "/" + mergeFileName)
        logInfo("[" + appName + "] rename: " + orcFile.toString + " => " + mergeFilePath.toString)
        fileSystem.rename(orcFile, mergeFilePath)
      })
      logInfo("[" + appName + "] move ORC File: " + (new Date().getTime - begin) + " ms")
    } else {
      for (file <- allFiles) {
        process(fileSystem.listStatus(file.getPath))
      }
    }
  }
}


