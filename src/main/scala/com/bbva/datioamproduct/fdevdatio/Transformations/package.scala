package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinType._
import com.bbva.datioamproduct.fdevdatio.common.fields._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{ col, collect_list}
import org.slf4j.{Logger, LoggerFactory}





package object Transformations {

  case class ReplaceColumnException(message: String, columnName: String, columns: Array[String])
    extends Exception(message)

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit class TransformationsDF(df: DataFrame) {



    @throws[Exception]
    def addColumn(newColumn: Column): DataFrame = {
      try{
        val columns: Array[Column] = df.columns.map(col) :+ newColumn
        df.select(columns: _*)
      } catch {
        case exception: Exception => throw exception
      }
    }

    @throws[ReplaceColumnException]
    def replaceColumn(field: Column): DataFrame = {
      val columnName: String = field.expr.asInstanceOf[NamedExpression].name

      if(df.columns.contains(columnName)) {
        val columns: Array[Column] = df.columns.map(nombre => {
          if (nombre != columnName) col(nombre) else field
        })
        df.select(columns: _*)
      } else {
        val message: String = s"La columna $columnName no puede ser remplazada"

        throw ReplaceColumnException(message, columnName, df.columns)
      }
    }






    def aggTitulo: DataFrame = {
      df.groupBy(VideoId.column)
        .agg(
          collect_list(Title.column).as("title2"))
    }


  }

  implicit class MapToDataFrame(dfMap: Map[String, DataFrame]) {
    def getFullDF: DataFrame = {
      dfMap(VideoInfoTag).join(dfMap(ChannelsTag), Seq(VideoId.name), LeftJoin)
    }
  }


  case class JointException(expectedKeys: Array[String],
                            columns: Array[String],
                            location: String = "com.bbva.datioaproduct.fdevdatio.MapToDataFrame.getFullDF",
                            message: String = "Ocurrio un error: ")
    extends Exception(message)


}
