package org.esgi.project.models


import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._


case class Movie(id: Int, title: String, viewCount: Long, stats: MovieStats)

object Movie {
  val movieReads: Reads[Movie] = (
    (JsPath \ "_id").read[Int] and
      (JsPath \ "title").read[String] and
      (JsPath \ "view_count").read[Long] and
      (JsPath \ "stats").read[MovieStats]
    ) (Movie.apply _)

  val movieWrites: Writes[Movie] = (
    (JsPath \ "_id").write[Int] and
      (JsPath \ "title").write[String] and
      (JsPath \ "view_count").write[Long] and
      (JsPath \ "stats").write[MovieStats]
    ) (unlift(Movie.unapply))

  implicit val movieFormat: Format[Movie] = Format(movieReads, movieWrites)

  def serdes: Serde[Movie] = {
    Serdes.fromFn[Movie](
      (value: Movie) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[Movie])
    )
  }
}
