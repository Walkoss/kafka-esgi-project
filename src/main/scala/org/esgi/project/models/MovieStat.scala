package org.esgi.project.models

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class MovieStat(startOnly: Int = 0, half: Int = 0, full: Int = 0)

object MovieStat {
  val movieStatsReads: Reads[MovieStat] = (
    (JsPath \ "start_only").read[Int] and
      (JsPath \ "half").read[Int] and
      (JsPath \ "full").read[Int]
    ) (MovieStat.apply _)

  val movieStatsWrites: Writes[MovieStat] = (
    (JsPath \ "start_only").write[Int] and
      (JsPath \ "half").write[Int] and
      (JsPath \ "full").write[Int]
    ) (unlift(MovieStat.unapply))

  implicit val movieStatFormat: Format[MovieStat] = Format(movieStatsReads, movieStatsWrites)
}