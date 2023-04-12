package com.test

import cats.implicits.toFunctorOps
import com.sun.org.slf4j.internal.LoggerFactory
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode
import io.circe.{Decoder, JsonObject}

import scala.util.{Failure, Success, Try}

sealed trait Impression

sealed case class ValidImpression(app_id: Int, advertiser_id: Int, country_code: String, id: String) extends Impression

sealed case class InvalidImpression(rawData: String) extends Impression

case class ImpressionBucket(validImpression: List[ValidImpression], invalidImpression: List[InvalidImpression])

object Impression {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val validImpressionDecoder: Decoder[ValidImpression] = deriveDecoder[ValidImpression]
  implicit val invalidImpressionDecoder: Decoder[InvalidImpression] =
    Decoder[JsonObject].map(obj => InvalidImpression(obj.toString()))
  implicit val fullImpressionDecoder: Decoder[Impression] = validImpressionDecoder.widen or invalidImpressionDecoder.widen

  def readImpressionsJson(filename: String): ImpressionBucket = {
    val source = scala.io.Source.fromFile(filename)
    Try(source.mkString) match {
      case Failure(_) =>
        source.close()
        logger.warn(s"Something went wrong while tiding file: $filename")
        ImpressionBucket.pristine
      case Success(value) =>
        decode[List[Impression]](value) match {
          case Left(_) =>
            logger.warn("Nothing in impressions json")
            ImpressionBucket.pristine
          case Right(impressions) => impressions.foldLeft(ImpressionBucket.pristine) {
            (bucket, impression) =>
              impression match {
                case valid: ValidImpression =>
                  ImpressionBucket(bucket.validImpression :+ valid, bucket.invalidImpression)
                case invalid: InvalidImpression =>
                  ImpressionBucket(bucket.validImpression, bucket.invalidImpression :+ invalid)
              }
          }
        }
    }
  }
}

object ImpressionBucket {
  def pristine: ImpressionBucket = ImpressionBucket(List.empty[ValidImpression], List.empty[InvalidImpression])

}
