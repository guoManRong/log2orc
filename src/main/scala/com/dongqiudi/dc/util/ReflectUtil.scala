package com.dongqiudi.dc.util

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentSkipListMap

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.{universe => ru}

/**
  * 反射工具类
  * Created by matrix on 17/10/8.
  */
object ReflectUtil extends Serializable with Logging {
  val runtimeMirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
  var moduleSymbol: ru.ModuleSymbol = _
  var moduleMirror: ru.ModuleMirror = _
  var instanceMirror: ru.InstanceMirror = _
  var methodSymbol: ru.MethodSymbol = _
  var methodMirror: ru.MethodMirror = _
  var termSymbol: ru.TermSymbol = _
  var struct: StructType = _
  var method: Method = _

  val fieldName = "struct"
  val methodName = "parseLog"
  val packageName = "com.dongqiudi.dc.etl.model."

  // 存储已获取的方法
  val methodMap = new ConcurrentSkipListMap[String, Method]

  /**
    * 动态获取日志模型相关方法和属性
    * @param logModel 日志模型
    * @return
    */
  def reflect(logModel: String): Unit = {
    try {
      logInfo("reflect " + logModel)
      moduleSymbol = runtimeMirror.staticModule(packageName + logModel)
      moduleMirror = runtimeMirror.reflectModule(moduleSymbol)
      instanceMirror = runtimeMirror.reflect(moduleMirror.instance)
      methodSymbol = moduleMirror.symbol.typeSignature.member(ru.newTermName(methodName)).asMethod
      methodMirror = instanceMirror.reflectMethod(methodSymbol)

      termSymbol = moduleMirror.symbol.typeSignature.member(ru.newTermName(fieldName)).asTerm
      struct = instanceMirror.reflectField(termSymbol).get.asInstanceOf[StructType]

      logInfo("methodMirror " + methodMirror)
      logInfo("struct " + struct)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(e.getMessage)
    }
  }

  /**
    * 动态调用解析日志的方法
    * @param line 日志
    * @return
    */
  def parseLog(logModel: String, line: String): Row = {
    var method = methodMap.get(logModel)
    if (method == null) {
      val logModelClass = Class.forName(packageName + logModel)
      method = logModelClass.getMethod(methodName, logModel.getClass)
      methodMap.put(logModel, method)
    }
    method.invoke(null, line).asInstanceOf[Row]
  }

  /**
    * 动态调用解析日志的方法
    * @param line 日志
    * @return
    */
  def parseLog2Array(logModel: String, line: String): Array[Row] = {
    var method = methodMap.get(logModel)
    if (method == null) {
      val logModelClass = Class.forName(packageName + logModel)
      method = logModelClass.getMethod(methodName, logModel.getClass)
      methodMap.put(logModel, method)
    }
    method.invoke(null, line).asInstanceOf[Array[Row]]
  }
}
