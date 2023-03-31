package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.CourseConfigConstants.{ChannelsTag, CountryTag, TrendingTag, VideoInfoTag, YearTag}
import com.bbva.datioamproduct.fdevdatio.utils.{IOUtils, SuperConfig}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_list, count, current_timestamp, date_format, desc, first, from_unixtime, max, min, to_date, unix_timestamp, when, year}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.slf4j.{Logger, LoggerFactory}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Player
import com.bbva.datioamproduct.fdevdatio.Transformations._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinType._
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.NumericConstants.{NegativeOne, Zero}
import com.bbva.datioamproduct.fdevdatio.common.fields._
import com.bbva.datioamproduct.fdevdatio.Transformations._
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import org.apache.hadoop.yarn.webapp.Params
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import shapeless.syntax.typeable.typeableOps

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}



class CourseSparkProcess extends SparkProcess with IOUtils{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {

    val config: Config = runtimeContext.getConfig

    val dfMap: Map[String, DataFrame] = config.readInputs


    val A: DataFrame = {
      dfMap(VideoInfoTag)
        .replaceColumn(TrendingDate())
        .addColumn(Year())
        .filter(Year.column === config.getInt(YearTag))
        .addColumn(DateRank())
        .filter(DateRank.column === 1)
        .addColumn(CategoryEvent())

    }
    A.show()

    val B: DataFrame = {
      dfMap(ChannelsTag).filter(Country.column === config.getString(CountryTag))
    }
    B.show()

    val C: DataFrame = A.join(B, Seq(VideoId.name), InnerJoin)
      .groupBy(VideoId.name,
        CategoryId.name,
        Views.name,
        Likes.name,
        Dislikes.name,
        TrendingDate.name,
        Year.name,
        ChannelTitle.name,
        Title.name,
        PublishTime.name,
        Country.name,
        CategoryEvent.name)
      .agg(max(Tags.name).as(Tags.name))

    C.show()

    val D: DataFrame = C.groupBy(
      VideoId.name,
      CategoryId.name,
      Views.name,
      Likes.name,
      Dislikes.name,
      TrendingDate.name,
      Year.name,
      ChannelTitle.name,
      PublishTime.name,
      Country.name,
      CategoryEvent.name,
      Tags.name)
     .agg(max(Title.name).as(Title.name))

    D.show()

    val E: DataFrame = D.orderBy(Views.column.cast("int").desc).addColumn(CountryName())

    E.show()
    E.printSchema()

    write(E, config.getConfig(TrendingTag))




   Zero
  }


  override def getProcessId: String = "CourseSparkProcess"
}
