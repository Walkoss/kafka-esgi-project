package org.esgi.project.models


import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.Serdes
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

case class View(id: Int, title: String, viewCategory: String)

object View {
  val viewReads: Reads[View] = (
    (JsPath \ "_id").read[Int] and
      (JsPath \ "title").read[String] and
      (JsPath \ "view_category").read[String]
    ) (View.apply _)

  val viewWrites: Writes[View] = (
    (JsPath \ "_id").write[Int] and
      (JsPath \ "title").write[String] and
      (JsPath \ "view_category").write[String]
    ) (unlift(View.unapply))

  implicit val viewFormat: Format[View] = Format(viewReads, viewWrites)

  def serdes: Serde[View] = {
    Serdes.fromFn[View](
      (value: View) => Json.stringify(Json.toJson(value)).getBytes,
      (byteArray: Array[Byte]) => Option(Json.parse(byteArray).as[View])
    )
  }
}
