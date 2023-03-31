package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{DevNameTag, InputTag}
import com.typesafe.config.Config
import org.apache.hadoop.yarn.webapp.Params
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.`set asScala`

package object utils {
  implicit class SuperConfig(config:Config) extends IOUtils {
    def readInputs: Map[String, DataFrame] = {
      config
        .getObject(InputTag)
        .keySet()
        .map(
          key => {
            val inputConfig: Config = config.getConfig(s"$InputTag.$key")
            (key, read(inputConfig))
          }
        ).toMap
    }
    def getDevName: String = {
      config.getString(DevNameTag)
    }



  }






}
