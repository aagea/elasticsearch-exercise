package es.alvaroagea.es.exercise.tests

import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass

object AbstractElasticSearchIT {

  /**
   * Elastic search helper.
   */
  val ESHelper : ElasticSearchHelper = this.initHelper()

  /**
   * Get the Elastic search target host.
   * @return The host specified in ES_HOST or the default host.
   */
  def getElasticSearchHost() : String = {
    sys.env.getOrElse(ElasticSearchHelper.HostEnvironmentVariable, ElasticSearchHelper.DefaultHost)
  }

  /**
   * Get the Elastic search target port.
   * @return The port specified in ES_PORT or the default port.
   */
  def getElasticSearchPort() : Int = {
    sys.env.getOrElse(ElasticSearchHelper.PortEnvironmentVariable,
      s"${ElasticSearchHelper.DefaultPort}").toInt
  }

  /**
   * Initialize the elastic search helper.
   * @return A Elastic search helper.
   */
  private def initHelper() : ElasticSearchHelper = {
    new ElasticSearchHelper(
      AbstractElasticSearchIT.getElasticSearchHost(),
      AbstractElasticSearchIT.getElasticSearchPort())
  }

  @BeforeClass
  def beforeElasticSearchIT(): Unit = {
    this.ESHelper.connect()
    Assert.assertTrue("Elastic search is not available", this.ESHelper.isConnected())
  }

  @AfterClass
  def afterRedisIT(): Unit = {
    this.ESHelper.close()
  }

}

/**
 * Abstract class to run ElasticSearch integration tests. ElasticSearch must be launched
 * before starting the tests.
 */
class AbstractElasticSearchIT extends {

  /**
   * Create a new index.
   * @param name The name of the index.
   * @return Whether the index has been created.
   */
  def createIndex(name: String) : Boolean = {
    AbstractElasticSearchIT.ESHelper.createIndex(name)
  }

  /**
   * Remove an index from Elastic Search.
   * @param name The name of the index.
   * @return Whether the index has been removed.
   */
  def removeIndex(name: String) : Boolean = {
    AbstractElasticSearchIT.ESHelper.removeIndex(name)
  }

  /**
   * Refresh an index.
   * @param name The name of the index.
   * @return Whether the index has been refreshed.
   */
  def refresh(name: String) : Boolean = {
    AbstractElasticSearchIT.ESHelper.refresh(name)
  }

}