package com.bbva.datioamproduct.fdevdatio.common

import org.apache.spark.internal.config


object StaticVals {
  case object CourseConfigConstants {
    val RootTag: String = "courseJob"
    val InputTag:String = s"$RootTag.input"
    val OutputTag:String = s"$RootTag.output"
    val ParamsTag: String = s"$RootTag.params"

    val ChannelsTag: String = "fdevChannels"
    val VideoInfoTag: String = "fdevVideoInfo"

    val TrendingTag: String = s"$OutputTag.fdevTrending"

    val DevNameTag: String = "courseJob.params.devName"
    val YearTag: String = s"$ParamsTag.year"
    val CountryTag: String = s"$ParamsTag.country"



  }

  case object NumericConstants {
    val NegativeOne: Int = -1
    val Zero: Int = 0
    val Twenty: Int = 20
    val Twenty_three: Int = 23
    val Thirty: Int = 30
    val Seventy: Int = 70
    val Eighty: Int = 80
    val One_hundred_sixty_five: Int = 165
    val One_hundred_seventy_five: Int = 175
    val One_hundred_eighty_five: Int = 185
    val Two_hundred: Int = 200
  }

  case object AlphabeticalConstants{
    val A: Char = 'A'
    val B: Char = 'B'
    val C: Char = 'C'
    val D: Char = 'D'
    val E: Char = 'E'

  }

  case object SpecialCharacters {
    val Comma: String = ","

  }

  case object JoinType {
    val LeftJoin: String = "left"
    val RightJoin: String = "Right"
    val InnerJoin: String = "inner"
    val OuterJoin: String = "outer"
    val LeftSemiJoin: String = "left_semi"
    val LeftAntiJoin: String = "left_anti"

  }


  case class Player(name: String, skill_ball_control: Int) extends Serializable
}
