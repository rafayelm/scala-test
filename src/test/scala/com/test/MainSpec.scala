package com.test

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class MainSpec extends AnyFlatSpec with Matchers {


  it should "return correct answer small agg" in {
    val data1 = List(
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "1"), List(BigDecimal(0.5))),
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "2"), List(BigDecimal(0.7), BigDecimal(1.2))),
      ImpressionWithRevenues(ValidImpression(2, 1, "US", "3"), List(BigDecimal(1.5), BigDecimal(2.0), BigDecimal(0.8)))
    )

    val expected1 = List(
      Metrics(1, "GB", 2, 3, 2.4),
      Metrics(2, "US", 1, 3, 4.3)
    )
    val actual = Main.aggregateImpressionsClicksRevenue(data1)

    actual.sortWith(_.toString > _.toString) shouldBe expected1.sortWith(_.toString > _.toString)
  }

  it should "return correct answer large and complex agg" in {
    val data2 = List(
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "1"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(2, 1, "US", "2"), List(BigDecimal(0.5))),
      ImpressionWithRevenues(ValidImpression(2, 1, "US", "3"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(3, 1, "FR", "4"), List(BigDecimal(1.2), BigDecimal(0.8))),
      ImpressionWithRevenues(ValidImpression(3, 1, "FR", "5"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(3, 1, "FR", "6"), List(BigDecimal(2.5), BigDecimal(1.7)))
    )
    val expected2: List[Metrics] = List(
      Metrics(1, "GB", 1, 0, BigDecimal(0)),
      Metrics(2, "US", 2, 1, BigDecimal(0.5)),
      Metrics(3, "FR", 3, 4, BigDecimal(6.2))
    )

    val actual = Main.aggregateImpressionsClicksRevenue(data2)

    actual.sortWith(_.toString > _.toString) shouldBe expected2.sortWith(_.toString > _.toString)
  }
  it should "return correct answer empty case" in {
    val data3 = List.empty
    val expected3 = List.empty

    val actual = Main.aggregateImpressionsClicksRevenue(data3)

    actual shouldBe expected3
  }

  "aggregateImpressionsClicksTopAdvertisers" should "correctly aggregate data and return top recommended advertiser ids" in {
    val data = List(
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "1"), List(BigDecimal(0.7), BigDecimal(1.2))),
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "2"), List(BigDecimal(0.7))),
      ImpressionWithRevenues(ValidImpression(1, 2, "GB", "3"), List(BigDecimal(1.5))),
      ImpressionWithRevenues(ValidImpression(2, 3, "US", "4"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(2, 3, "US", "5"), List(BigDecimal(0.9))),
      ImpressionWithRevenues(ValidImpression(2, 3, "US", "6"), List(BigDecimal(1.5), BigDecimal(2.0)))
    )

    val expected = List(
      Recommendations(1, "GB", List(2, 1)),
      Recommendations(2, "US", List(3))
    )

    val result = Main.aggregateImpressionsClicksTopAdvertisers(data)
    val actual = result

    actual.sortWith(_.toString > _.toString) shouldBe expected.sortWith(_.toString > _.toString)
  }

  "aggregateImpressionsClicksTopAdvertisers" should "return empty list if input list is empty" in {
    val data = List.empty[ImpressionWithRevenues]
    val expected = List.empty[Recommendations]
    val result = Main.aggregateImpressionsClicksTopAdvertisers(data)

    result shouldBe expected
  }

  "aggregateImpressionsClicksTopAdvertisers" should "return empty list if there are no clicks for any impression" in {
    val data = List(
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "1"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(1, 1, "GB", "2"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(1, 2, "GB", "3"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(2, 3, "US", "4"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(2, 3, "US", "5"), List.empty[BigDecimal]),
      ImpressionWithRevenues(ValidImpression(2, 3, "US", "6"), List.empty[BigDecimal])
    )
    val expected = List.empty[Recommendations]
    val result = Main.aggregateImpressionsClicksTopAdvertisers(data)

    result shouldBe expected
  }


  val impression1 = ValidImpression(1, 1, "GB", "1")
  val impression2 = ValidImpression(2, 1, "US", "2")
  val click1 = ValidClick("1", BigDecimal(1.23))
  val click2 = ValidClick("2", BigDecimal(4.56))
  val click3 = ValidClick("3", BigDecimal(7.89))

  "joinImpressionsToClicks" should "return empty list if impressions and clicks are empty" in {
    val clicks = List.empty[ValidClick]
    val impressions = List.empty[ValidImpression]
    val result = Main.joinImpressionsToClicks(clicks, impressions)

    result shouldBe empty
  }

  "joinImpressionsToClicks" should "return impressions without clicks if there are no clicks for the impressions" in {
    val clicks = List(click3)
    val impressions = List(impression1, impression2)
    val result = Main.joinImpressionsToClicks(clicks, impressions)

    result shouldBe empty
  }

  "joinImpressionsToClicks" should "return impressions with clicks if there are clicks for the impressions" in {
    val clicks = List(click1, click2, click3)
    val impressions = List(impression1, impression2)
    val result = Main.joinImpressionsToClicks(clicks, impressions)
    val expected = List(ImpressionWithRevenues(impression1, List(BigDecimal(1.23))), ImpressionWithRevenues(impression2, List(BigDecimal(4.56))))

    result.sortWith(_.toString > _.toString) shouldBe expected.sortWith(_.toString > _.toString)
  }

  "joinImpressionsToClicks" should "handle edge case with empty revenue list" in {
    val clicks = List(ValidClick("1", BigDecimal(0.0)), ValidClick("1", BigDecimal(0.0)))
    val impressions = List(impression1)
    val result = Main.joinImpressionsToClicks(clicks, impressions)
    val expected = List(ImpressionWithRevenues(impression1, List(BigDecimal(0.0), BigDecimal(0.0))))

    result shouldBe expected
  }

  "joinImpressionsToClicks" should "handle edge case with empty impressions list" in {
    val clicks = List(click1, click2)
    val impressions = List.empty[ValidImpression]
    val result = Main.joinImpressionsToClicks(clicks, impressions)

    result shouldBe empty
  }
}
