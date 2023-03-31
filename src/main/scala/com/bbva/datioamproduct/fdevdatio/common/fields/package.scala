package com.bbva.datioamproduct.fdevdatio.common


import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.{ col,  concat_ws,  rank,  row_number, to_date, when, year}


package object fields {




  case object ViewsRank extends Field {
    override val name: String = "view_rank"

    def apply(): Column = {
      val w: WindowSpec = Window.orderBy(Views.column.cast("int").desc)
      rank().over(w).alias(name)
    }
  }

  case object LikesRank extends Field {
    override val name: String = "like_rank"

    def apply(): Column = {
      val w: WindowSpec = Window.orderBy(Likes.column.cast("int").desc)
      rank().over(w).alias(name)
    }
  }

  case object HateRank extends Field {
    override val name: String = "dislike_rank"

    def apply(): Column = {
      val w: WindowSpec = Window.orderBy(Dislikes.column.cast("int").desc)
      rank().over(w).alias(name)
    }
  }


  case object CategoryEvent extends Field {
    override val name: String = "category_event"

    def apply(): Column = {
      concat_ws(",",
        when(ViewsRank() <= 10, ("1")).otherwise(null),
        when(HateRank() <= 10, ("2")).otherwise(null),
        when(LikesRank() <= 10, ("3")).otherwise(null)
      ) alias name
    }
  }

  case object CountryName extends Field {
    override val name: String = "country_name"

    def apply(): Column = {
        when(Country.column === "CA", ("Canadá"))
        .when(Country.column === "DE", ("Alemania"))
        .when(Country.column === "FR", ("Francia"))
        .when(Country.column === "GB", (" Gran Bretaña"))
        .when(Country.column === "US", ("Estados Unidos"))
        .when(Country.column === "JP", ("Japón"))
        .when(Country.column === "MX", ("México"))
        .when(Country.column === "RU", ("Rusia"))
        .when(Country.column === "IN", ("India"))
        .otherwise("") alias name
    }
  }

  case object TrendingDate extends Field {
    override val name: String = "trending_date"

    def apply(): Column = {
      to_date(TrendingDate.column, "yy.dd.MM") alias name
    }
  }

  case object Year extends Field {
    override val name: String = "year"

    def apply(): Column = {
      year(TrendingDate.column) alias name
    }
  }

  case object DateRank extends Field {
    override val name: String = "date_rank"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy("video_id").orderBy(col("trending_date").desc)
      row_number().over(w).alias(name)

    }
  }

  case object Views extends Field {
    override val name: String = "views"
  }

  case object Dislikes extends Field {
    override val name: String = "dislikes"
  }

  case object Likes extends Field {
    override val name: String = "likes"
  }

  case object Title extends Field {
    override val name: String = "title"

  }

  case object VideoId extends Field {
    override val name: String = "video_id"
  }

  case object CategoryId extends Field {
    override val name: String = "category_id"
  }

  case object ChannelTitle extends Field {
    override val name: String = "channel_title"
  }

  case object PublishTime extends Field {
    override val name: String = "publish_time"
  }

  case object Country extends Field {
    override val name: String = "country"
  }

  case object Tags extends Field {
    override val name: String = "tags"
  }



}
