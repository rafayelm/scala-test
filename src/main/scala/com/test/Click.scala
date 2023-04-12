package com.test

import cats.implicits.toFunctorOps
import com.sun.org.slf4j.internal.LoggerFactory
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode
import io.circe.{Decoder, JsonObject}

import scala.util.{Failure, Success, Try}

sealed trait Click

sealed case class ValidClick(impression_id: String, revenue: BigDecimal) extends Click

sealed case class InvalidClick(rawData: String) extends Click

object Click {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val validClickDecoder: Decoder[ValidClick] = deriveDecoder[ValidClick]
  implicit val invalidClickDecoder: Decoder[InvalidClick] =
    Decoder[JsonObject].map(obj => InvalidClick(obj.toString()))
  implicit val fullClickDecoder: Decoder[Click] = validClickDecoder.widen or invalidClickDecoder.widen

  def readClicksJson(filename: String): ClickBucket = {
    val source = scala.io.Source.fromFile(filename)
    Try(source.mkString) match {
      case Failure(_) =>
        source.close()
        logger.warn(s"Something went wrong while tiding file: $filename")
        ClickBucket.pristine
      case Success(value) =>
        decode[List[Click]](value) match {
          case Left(_) =>
            logger.warn("Nothing in clicks json")
            ClickBucket.pristine
          case Right(clicks) => clicks.foldLeft(ClickBucket.pristine) {
            (bucket, click) =>
              click match {
                case valid: ValidClick =>
                  ClickBucket(bucket.validClicks :+ valid, bucket.invalidClicks)
                case invalid: InvalidClick =>
                  ClickBucket(bucket.validClicks, bucket.invalidClicks :+ invalid)
              }
          }
        }
    }
  }
}

case class ClickBucket(validClicks: List[ValidClick], invalidClicks: List[InvalidClick])

object ClickBucket {
  def pristine: ClickBucket = ClickBucket(List.empty[ValidClick], List.empty[InvalidClick])


}