package org.esgi.project.models


import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._


case class MovieLike(title: String, score: Double)

object MovieLike {
  val movieWithLikeReads: Reads[MovieLike] = (
    (JsPath \ "title").read[String] and
      (JsPath \ "score").read[Double]
    ) (MovieLike.apply _)

  val movieWithLikeWrites: OWrites[MovieLike] = (
    (JsPath \ "title").write[String] and
      (JsPath \ "score").write[Double]
    ) (unlift(MovieLike.unapply))

  implicit val movieWithLikeFormat: Format[MovieLike] = Format(movieWithLikeReads, movieWithLikeWrites)

  def serdes: Serde[MovieLike] = {
    Serdes.fromFn[MovieLike](
      (value: MovieLike) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[MovieLike])
    )
  }
}