package org.esgi.project.models

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class Like(id: Int, score: Double)

object Like {
  val likeReads: Reads[Like] = (
    (JsPath \ "_id").read[Int] and
      (JsPath \ "score").read[Double]
    ) (Like.apply _)

  val likeWrites: Writes[Like] = (
    (JsPath \ "_id").write[Int] and
      (JsPath \ "score").write[Double]
    ) (unlift(Like.unapply))

  implicit val likeFormat: Format[Like] = Format(likeReads, likeWrites)

  def serdes: Serde[Like] = {
    Serdes.fromFn[Like](
      (value: Like) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[Like])
    )
  }
}
