package org.esgi.project.models

import play.api.libs.json._

case class Error(message: String)

object Error {
  implicit val format: OFormat[Error] = Json.format[Error]
}
