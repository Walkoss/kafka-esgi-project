package org.esgi.project.models

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class MovieStats(past: Stat, lastMinute: Stat, lastFiveMinutes: Stat)


object MovieStats {
  val reads: Reads[MovieStats] = (
    (JsPath \ "past").read[Stat] and
      (JsPath \ "last_minute").read[Stat] and
      (JsPath \ "last_five_minutes").read[Stat]
    ) (MovieStats.apply _)

  val writes: Writes[MovieStats] = (
    (JsPath \ "past").write[Stat] and
      (JsPath \ "last_minute").write[Stat] and
      (JsPath \ "last_five_minutes").write[Stat]
    ) (unlift(MovieStats.unapply))

  implicit val format: Format[MovieStats] = Format(reads, writes)
}