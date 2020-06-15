package com.dongqiudi.dc.util

import java.io.{FileNotFoundException, IOException}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil

/**
  * 文件操作相关
  * Created by matrix on 17/8/29.
  */
object FileUtil extends Serializable with Logging {
  val hiveWarehousePath = "/user/hive/warehouse/"

  /**
    * 根据文件大小构建coalesce
    *
    * @param fileSystem     文件系统
    * @param filePath       文件路径
    * @param coalesceSize   收敛大小
    * @return
    */
  def makeCoalesce(fileSystem: FileSystem, filePath: String, coalesceSize: Int): Int = {
    var partitions = 0l
    for (path <- filePath.split(",")) {
      fileSystem.globStatus(new Path(path)).map(x => {
        partitions += x.getLen
        null
      })
    }
    logInfo("partitions: " + partitions)
    (partitions / 1024 / 1024 / coalesceSize).toInt + 1
  }

  /**
    * 将每个小时下的 ORC 文件合并生成到对应的 Hive 目录下
    * @param fileSystem     文件系统
    * @param outputPath     输出路径
    * @param appName        应用名称
    * @param hiveTablePath  hive 表目录
    */
  def moveORCFiles(fileSystem: FileSystem, outputPath: String, appName: String, hiveTablePath: String, partitions: String): Unit = {
    try {
      // /spark/app/etl/business/logic_ap/2017093004/day=20170930/hour=04/part-r-00000-59eb4e04-9267-4f31-89e7-e4d2288cd51c.orc
      val orcFiles = SparkHadoopUtil.get.globPath(new Path(outputPath + ORCConfigUtil.getTemplate(partitions) + "/*.orc"))
      var hivePartition = ""
      val array = outputPath.split("/")
      val loadTime = array(array.length-1)
      orcFiles.foreach(orcFile => {
        // /spark/app/etl/business/logic_ap/2017093004/day=20170930/hour=04/part-r-00000-59eb4e04-9267-4f31-89e7-e4d2288cd51c.orc
        // =>
        // /user/hive/warehouse/business.db/logic_ap/day=20170930/hour=04/00000.orc
        val fullName = orcFile.toString
        logInfo("orcFile: " + fullName)

        val partition = fullName.substring(fullName.indexOf(outputPath) + outputPath.length - 1, fullName.lastIndexOf("/")+1)
        val temp = partition.substring(1, partition.length-1).replace("/", ", ")
        if (!temp.equals(hivePartition)) {
          hivePartition = temp
          logInfo("hivePartition: " + hivePartition)

          val deleteFilePath = hiveWarehousePath + hiveTablePath + partition + "*" + loadTime + ".orc"
          logInfo("deleteFilePath: " + deleteFilePath)
          SparkHadoopUtil.get.globPath(new Path(deleteFilePath)).map(x => fileSystem.delete(x))
        }

        val fileName = fullName.substring(fullName.lastIndexOf("/")+1)
        val fileFlag = fileName.split("-")(2)

        val targetFileName = hiveWarehousePath + hiveTablePath + partition + fileFlag + "-" + loadTime + ".orc"
        logInfo("targetFileName: " + targetFileName)
        val targetFile = new Path(targetFileName)
        fileSystem.delete(targetFile)

        if (!fileSystem.exists(targetFile.getParent)) {
          fileSystem.mkdirs(targetFile.getParent)
        }

        fileSystem.rename(orcFile, targetFile)
      })
    } catch {
      case _: FileNotFoundException =>
        logError("[" + appName + "] Error: Not Found ORC File")
      case _: IOException =>
        logError("[" + appName + "] Error: ORC File Merge Error")
      case e: Exception =>
        logError("[" + appName + "] Error: " + e.getMessage)
    }
  }
}
