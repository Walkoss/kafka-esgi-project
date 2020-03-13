package org.esgi.project.models

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class TopLikes(title: String, score: Double)

object TopLikes {
  val reads: Reads[TopLikes] = (
    (JsPath \ "title").read[String] and
      (JsPath \ "score").read[Double]
    ) (TopLikes.apply _)

  val writes: Writes[TopLikes] = (
    (JsPath \ "title").write[String] and
      (JsPath \ "score").write[Double]
    ) (unlift(TopLikes.unapply))

  implicit val viewFormat: Format[TopLikes] = Format(reads, writes)

  def serdes: Serde[TopLikes] = {
    Serdes.fromFn[TopLikes](
      (value: TopLikes) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[TopLikes])
    )
  }
}
