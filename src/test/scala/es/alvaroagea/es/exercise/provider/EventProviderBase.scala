package es.alvaroagea.es.exercise.provider

import java.time.Instant
import java.time.temporal.ChronoUnit

import org.junit.Assert
import org.junit.Test


trait EventProviderBase {

  val provider: EventProvider

  val initTime = Instant.parse("2007-12-03T10:15:30.00Z")

  def init(): Unit = {
    List(
      Event("A", "1", initTime.plus(1, ChronoUnit.MINUTES), initTime.plus(2, ChronoUnit.MINUTES)),
      Event("B", "2", initTime.plus(3, ChronoUnit.MINUTES), initTime.plus(4, ChronoUnit.MINUTES)),
      Event("A", "3", initTime.plus(5, ChronoUnit.MINUTES), initTime.plus(5, ChronoUnit.MINUTES)),
      Event("B", "1", initTime.plus(7, ChronoUnit.MINUTES), initTime.plus(8, ChronoUnit.MINUTES)),
      Event("A", "2", initTime.plus(9, ChronoUnit.MINUTES), initTime.plus(10, ChronoUnit.MINUTES)),
      Event("B", "3", initTime.plus(11, ChronoUnit.MINUTES), initTime.plus(12, ChronoUnit.MINUTES)),
      Event("A", "1", initTime.plus(13, ChronoUnit.MINUTES), initTime.plus(14, ChronoUnit.MINUTES)),
      Event("B", "2", initTime.plus(15, ChronoUnit.MINUTES), initTime.plus(16, ChronoUnit.MINUTES)),
      Event("A", "3", initTime.plus(17, ChronoUnit.MINUTES), initTime.plus(18, ChronoUnit.MINUTES)),
      Event("B", "1", initTime.plus(19, ChronoUnit.MINUTES), initTime.plus(20, ChronoUnit.MINUTES)),
      Event("A", "2", initTime.plus(21, ChronoUnit.MINUTES), initTime.plus(22, ChronoUnit.MINUTES)),
      Event("B", "3", initTime.plus(23, ChronoUnit.MINUTES), initTime.plus(24, ChronoUnit.MINUTES)),
      Event("A", "1", initTime.plus(25, ChronoUnit.MINUTES), initTime.plus(26, ChronoUnit.MINUTES)),
      Event("B", "2", initTime.plus(27, ChronoUnit.MINUTES), initTime.plus(28, ChronoUnit.MINUTES)),
      Event("A", "3", initTime.plus(29, ChronoUnit.MINUTES), initTime.plus(30, ChronoUnit.MINUTES)),
      Event("B", "1", initTime.plus(31, ChronoUnit.MINUTES), initTime.plus(32, ChronoUnit.MINUTES)),
      Event("A", "2", initTime.plus(33, ChronoUnit.MINUTES), initTime.plus(34, ChronoUnit.MINUTES)),
      Event("B", "3", initTime.plus(35, ChronoUnit.MINUTES), initTime.plus(36, ChronoUnit.MINUTES)),
      Event("A", "1", initTime.plus(37, ChronoUnit.MINUTES), initTime.plus(38, ChronoUnit.MINUTES)),
      Event("B", "2", initTime.plus(39, ChronoUnit.MINUTES), initTime.plus(40, ChronoUnit.MINUTES))
    ).foreach((ev: Event) => {
      val result = this.provider.persist(ev)
      Assert.assertTrue("Resul must be defined.", result.isDefined)
    })
  }


  @Test
  def lastBasicTest(): Unit = {
    val result = this.provider.last("A", 4)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 4", 4, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "1",
        initTime.plus(25, ChronoUnit.MINUTES), initTime.plus(26, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "1",
        initTime.plus(37, ChronoUnit.MINUTES), initTime.plus(38, ChronoUnit.MINUTES)),
      result.get.get(3)
    )
  }

  @Test
  def lastNotFoundTest(): Unit = {
    val result = this.provider.last("C", 4)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }


  @Test
  def lastWithTagsTest(): Unit = {
    val result = this.provider.last("A", Array("1", "2"), 2, None)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "2", initTime.plus(33, ChronoUnit.MINUTES), initTime.plus(34, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "1", initTime.plus(37, ChronoUnit.MINUTES), initTime.plus(38, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def lastWithTagsNotFoundTest(): Unit = {
    val result = this.provider.last("C", Array("4"), 2)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }

  @Test
  def lastWithTagsAndBeforeTest(): Unit = {
    val result = this.provider.last("A", Array("1", "2"), 2,
      Some(initTime.plus(37, ChronoUnit.MINUTES)))
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "1", initTime.plus(25, ChronoUnit.MINUTES), initTime.plus(26, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "2", initTime.plus(33, ChronoUnit.MINUTES), initTime.plus(34, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def lastWithDistinctTest(): Unit = {
    val result = this.provider.lastDistinct("A", Array("1", "2"), 2, None)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "3", initTime.plus(17, ChronoUnit.MINUTES), initTime.plus(18, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "3", initTime.plus(29, ChronoUnit.MINUTES), initTime.plus(30, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def lastWithDistinctAndBeforeTest(): Unit = {
    val result = this.provider.lastDistinct("A", Array("1", "2"), 2,
      Some(initTime.plus(29, ChronoUnit.MINUTES)))
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "3", initTime.plus(5, ChronoUnit.MINUTES), initTime.plus(5, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "3", initTime.plus(17, ChronoUnit.MINUTES), initTime.plus(18, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def lastDistinctNotFoundTest(): Unit = {
    val result = this.provider.lastDistinct("A", Array("1", "2", "3"), 5)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }

  @Test
  def searchTest(): Unit = {
    val result = this.provider.search("A", Array("1", "2"),
      initTime.plus(37, ChronoUnit.MINUTES), initTime.plus(23, ChronoUnit.MINUTES), 5)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "1", initTime.plus(25, ChronoUnit.MINUTES), initTime.plus(26, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "2", initTime.plus(33, ChronoUnit.MINUTES), initTime.plus(34, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def searchDistinctTest(): Unit = {
    val result = this.provider.searchDistinct("A", Array("1", "2"),
      initTime.plus(29, ChronoUnit.MINUTES), initTime.plus(3, ChronoUnit.MINUTES), 5)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "3", initTime.plus(5, ChronoUnit.MINUTES), initTime.plus(5, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "3", initTime.plus(17, ChronoUnit.MINUTES), initTime.plus(18, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def searchNotFoundTest(): Unit = {
    val result = this.provider.search("C", Array("4"),
      initTime.plus(29, ChronoUnit.MINUTES), initTime.plus(3, ChronoUnit.MINUTES), 5)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }

  @Test
  def searchDistinctNotFoundTest(): Unit = {
    val result = this.provider.searchDistinct("A", Array("1", "2", "3"),
      initTime.plus(29, ChronoUnit.MINUTES), initTime.plus(3, ChronoUnit.MINUTES), 5)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }


  @Test
  def firstTest(): Unit = {
    val result = this.provider.first("A", Array("1", "2"), 2, None)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "1", initTime.plus(1, ChronoUnit.MINUTES), initTime.plus(2, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "2", initTime.plus(9, ChronoUnit.MINUTES), initTime.plus(10, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def firstNotFoundTest(): Unit = {
    val result = this.provider.first("C", Array("4"), 2)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }

  @Test
  def firstAndBeforeTest(): Unit = {
    val result = this.provider.first("A", Array("1", "2"), 2,
      Some(initTime.plus(2, ChronoUnit.MINUTES)))
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "2", initTime.plus(9, ChronoUnit.MINUTES), initTime.plus(10, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "1", initTime.plus(13, ChronoUnit.MINUTES), initTime.plus(14, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def firstWithDistinctTest(): Unit = {
    val result = this.provider.firstDistinct("A", Array("1", "2"), 2, None)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "3", initTime.plus(5, ChronoUnit.MINUTES), initTime.plus(5, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "3", initTime.plus(17, ChronoUnit.MINUTES), initTime.plus(18, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def firstWithDistinctAndBeforeTest(): Unit = {
    val result = this.provider.firstDistinct("A", Array("1", "2"), 2,
      Some(initTime.plus(5, ChronoUnit.MINUTES)))
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertEquals("Size must be 2", 2, result.get.size())

    Assert.assertEquals("First event must be equals",
      Event("A", "3", initTime.plus(17, ChronoUnit.MINUTES), initTime.plus(18, ChronoUnit.MINUTES)),
      result.get.get(0)
    )
    Assert.assertEquals("Last event must be equals",
      Event("A", "3", initTime.plus(29, ChronoUnit.MINUTES), initTime.plus(30, ChronoUnit.MINUTES)),
      result.get.get(1)
    )
  }

  @Test
  def firstDistinctNotFoundTest(): Unit = {
    val result = this.provider.firstDistinct("A", Array("1", "2", "3"), 5)
    Assert.assertTrue("Result has been defined", result.isDefined)
    Assert.assertTrue("Result has been empty", result.get.isEmpty)
  }

}
