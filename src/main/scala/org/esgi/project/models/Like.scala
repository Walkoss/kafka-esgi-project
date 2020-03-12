package org.esgi.project.models

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
}
