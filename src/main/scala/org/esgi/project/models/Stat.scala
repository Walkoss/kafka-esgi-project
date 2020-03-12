package org.esgi.project.models

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class Stat(startOnly: Int = 0, half: Int = 0, full: Int = 0)

object Stat {
  val reads: Reads[Stat] = (
    (JsPath \ "start_only").read[Int] and
      (JsPath \ "half").read[Int] and
      (JsPath \ "full").read[Int]
    ) (Stat.apply _)

  val writes: Writes[Stat] = (
    (JsPath \ "start_only").write[Int] and
      (JsPath \ "half").write[Int] and
      (JsPath \ "full").write[Int]
    ) (unlift(Stat.unapply))

  implicit val format: Format[Stat] = Format(reads, writes)

  def serdes: Serde[Stat] = {
    Serdes.fromFn[Stat](
      (value: Stat) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[Stat])
    )
  }
}