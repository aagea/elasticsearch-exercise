package es.alvaroagea.es.exercise.tests

import es.alvaroagea.es.exercise.manager.BasicElasticSearchSession
import es.alvaroagea.es.exercise.manager.ElasticSearchSessionManager
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object AbstractElasticSearchManagerIT {

  /**
   * Target session
   */
  val TargetSession: String = "it-session"

  /**
   * Elastic search session.
   */
  val Session: BasicElasticSearchSession = new BasicElasticSearchSession(
    this.TargetSession,
    AbstractElasticSearchIT.getElasticSearchHost(),
    AbstractElasticSearchIT.getElasticSearchPort()
  )

  /**
   * Class logger.
   */
  private val Log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
   * Initialize the ElasticSearch session.
   */
  @BeforeClass
  def beforeClassElasticSearchManagerIT(): Unit = {
    Log.info("Starting Elastic Search Session Manager.")
    ElasticSearchSessionManager.init(this.Session)
    Assert.assertTrue("Default session must be defined",
      ElasticSearchSessionManager.getSession(this.TargetSession).isDefined)
  }

  /**
   * Finish ElasticSearch Session Manager.
   */
  @AfterClass
  def afterClassElasticSearchManagerIT(): Unit = {
    Log.info("Stopping ElasticSearch Session Manager.")
    ElasticSearchSessionManager.close(this.TargetSession)
  }
}

/**
 * Abstract class to be inherited in all ElasticSearch tests.
 */
abstract class AbstractElasticSearchManagerIT extends AbstractElasticSearchIT
