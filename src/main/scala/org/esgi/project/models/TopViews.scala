package org.esgi.project.models

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class TopViews(title: String, views: Long)

object TopViews {
  val reads: Reads[TopViews] = (
    (JsPath \ "title").read[String] and
      (JsPath \ "views").read[Long]
    ) (TopViews.apply _)

  val writes: Writes[TopViews] = (
    (JsPath \ "title").write[String] and
      (JsPath \ "views").write[Long]
    ) (unlift(TopViews.unapply))

  implicit val viewFormat: Format[TopViews] = Format(reads, writes)

  def serdes: Serde[TopViews] = {
    Serdes.fromFn[TopViews](
      (value: TopViews) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[TopViews])
    )
  }
}
