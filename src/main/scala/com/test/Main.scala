package com.test

import com.sun.org.slf4j.internal.LoggerFactory
import io.circe.generic.auto._
import io.circe.syntax._
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException

import java.io.File


case class ImpressionWithRevenues(impression: ValidImpression, revenues: List[BigDecimal])

case class AppIdWithCountryCode(appId: Int, countryCode: String)

case class ImpressionsWithRevenueRatiosByAppIdAndCountryCode(
                                                              appIdWithCountryCode: AppIdWithCountryCode,
                                                              impressionsWithRevenueRatio: List[ImpressionsWithRevenueRatio]
                                                            )


case class ImpressionsWithRevenueRatio(app_id: Int, country_code: String, advertiser_id: Int, revenueRatio: BigDecimal)

case class Metrics(app_id: Int, country_code: String, impressions: Int, clicks: Int, revenue: BigDecimal)

case class Recommendations(app_id: Int, country_code: String, recommended_advertiser_ids: List[Int])

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      logger.error("""Arg input error. There should be 3 arguments provided:
        1. Path to the folder
        2. Name of the file containing clicks
        3. Name of the file containing impressions.""")
      throw new ValueException("Incorrect arguments provided.")
    }

    val Array(folderPath, clicksFileName, impressionsFileName) = args
    val fileSeparator = File.separator

    val clicksPath = s"$folderPath$fileSeparator$clicksFileName"
    val impressionsPath = s"$folderPath$fileSeparator$impressionsFileName"
    val metricsPath = s"$folderPath${fileSeparator}metrics.json"
    val recommendationsPath = s"$folderPath${fileSeparator}recommendations.json"
    val clickBucket = Click.readClicksJson(clicksPath)
    val clicks = clickBucket.validClicks
    val invalidClicks = clickBucket.invalidClicks
    // the invalid clicks and impressions will be logged in this program,
    // but one can write them to the file if need be.
    if (invalidClicks.nonEmpty) {
      logger.error(s"unusable records from click: $invalidClicks")
    }
    val impressionBucket = Impression.readImpressionsJson(impressionsPath)
    val impressions = impressionBucket.validImpression
    val invalidImpressions = impressionBucket.invalidImpression
    if (invalidImpressions.nonEmpty) {
      logger.error(s"unusable records from impression: $invalidImpressions")
    }
    val joinedImpressionsClicks = joinImpressionsToClicks(clicks, impressions)
    val metricsAggRes = aggregateImpressionsClicksRevenue(joinedImpressionsClicks)
    val recommendationsAggRes = aggregateImpressionsClicksTopAdvertisers(joinedImpressionsClicks)

    writeJsonFile(metricsPath, metricsAggRes)
    writeJsonFile(recommendationsPath, recommendationsAggRes)
  }


  def joinImpressionsToClicks(clicks: List[ValidClick], impressions: List[ValidImpression]): List[ImpressionWithRevenues] = {
    val clickMap = clicks.groupBy(_.impression_id)
    for {
      impression <- impressions
      clicks <- clickMap.get(impression.id)
      revenues = clicks.map(_.revenue)
    } yield ImpressionWithRevenues(impression, revenues)
  }


  def aggregateImpressionsClicksRevenue(data: List[ImpressionWithRevenues]): List[Metrics] = {
    val groupedData = data.groupBy(impClick => (impClick.impression.app_id, impClick.impression.country_code))

    groupedData.map {
      case ((app_id, country_code), impClicks) =>
        val impressions = impClicks.length
        val clicks = impClicks.map(_.revenues).map(_.length).sum
        val revenue = impClicks.flatMap(_.revenues).sum
        Metrics(app_id, country_code, impressions, clicks, revenue)
    }.toList
  }

  def aggregateImpressionsClicksTopAdvertisers(data: List[ImpressionWithRevenues]): List[Recommendations] = {
    data
      .filter(i => i.revenues.nonEmpty)
      .groupBy(impClick => (impClick.impression.app_id, impClick.impression.country_code, impClick.impression.advertiser_id))
      .map { case ((app_id, country_code, advertiser_id), impClicks) =>
        val impressions = impClicks.length
        val revenue = impClicks.flatMap(_.revenues).sum
        ImpressionsWithRevenueRatio(app_id, country_code, advertiser_id, revenue / impressions)
      }
      .groupBy(ir => AppIdWithCountryCode(ir.app_id, ir.country_code))
      .map(v => ImpressionsWithRevenueRatiosByAppIdAndCountryCode(v._1, v._2.toList))
      .map(k => Recommendations(
        k.appIdWithCountryCode.appId,
        k.appIdWithCountryCode.countryCode,
        k.impressionsWithRevenueRatio
          .sortWith(_.revenueRatio > _.revenueRatio)
          .take(5)
          .map(_.advertiser_id))
      )
      .toList
  }

  def writeJsonFile[T](filename: String, data: T)(implicit encoder: io.circe.Encoder[T]): Unit = {
    val json = data.asJson
    val pw = new java.io.PrintWriter(new File(filename))
    try pw.write(json.spaces2) finally pw.close()
  }
}