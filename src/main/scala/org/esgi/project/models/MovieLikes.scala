package org.esgi.project.models


import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._


case class MovieLikes(title: String = "", movieLikesCount: Int = 0, score: Double = 0)


object MovieLikes {
  val likeReads: Reads[MovieLikes] = (
    (JsPath \ "title").read[String] and
      (JsPath \ "movie_likes_count").read[Int] and
      (JsPath \ "score").read[Double]
    ) (MovieLikes.apply _)

  val likeWrites: Writes[MovieLikes] = (
    (JsPath \ "title").write[String] and
      (JsPath \ "movie_likes_count").write[Int] and
      (JsPath \ "score").write[Double]
    ) (unlift(MovieLikes.unapply))

  implicit val likeFormat: Format[MovieLikes] = Format(likeReads, likeWrites)

  def serdes: Serde[MovieLikes] = {
    Serdes.fromFn[MovieLikes](
      (value: MovieLikes) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[MovieLikes])
    )
  }
}
