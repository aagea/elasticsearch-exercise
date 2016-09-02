package es.alvaroagea.es.exercise.manager

import java.util.HashMap
import java.util.List
import java.util.Map

import es.alvaroagea.es.exercise.utils.JavaCollectionsConversions.scalaToJavaConsumer
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Elastic Search session manager.
 */
object ElasticSearchSessionManager {

  /**
   * Class logger.
   */
  private val Log: Logger = LoggerFactory.getLogger(this.getClass.getName)

  /**
   * Map of clients indexed by session identifier.
   */
  private val Sessions: Map[String, TransportClient] = new HashMap[String, TransportClient]()

  /**
   * Initialize a list of sessions.
   * @param sessions The sessions.
   * @return Whether all the sessions have been initialized.
   */
  def init(sessions: List[BasicElasticSearchSession]): Boolean = {
    val it = sessions.iterator()
    var result = true
    while (result && it.hasNext) {
      result = ElasticSearchSessionManager.init(it.next())
    }
    result
  }

  /**
   * Initialize the session.
   * @param session The session definition.
   * @return Whether it has been initialized or not.
   */
  def init(session: BasicElasticSearchSession): Boolean = {
    if (this.Sessions.containsKey(session.id)) {
      throw new UnsupportedOperationException(
        s"A session ${session.id} already exists")
    }

    val settings: Settings = Settings.settingsBuilder()
      .put("client.transport.sniff", true).build()
    var builder = TransportClient.builder().settings(settings).build()
    session.asTransportAddresses().forEach((address: InetSocketTransportAddress) => {
      builder = builder.addTransportAddress(address)
    })
    if (builder.connectedNodes().size() > 0) {
      this.Sessions.put(session.id, builder)
      true
    } else {
      Log.error(s"Cannot connect to ElasticSearch with: ${session}")
      builder.close()
      false
    }
  }

  /**
   * Get the transport client associated with a given session.
   * @param sessionId The session identifier.
   * @return An option with the transport client.
   */
  def getSession(sessionId: String): Option[TransportClient] = {
    Option(this.Sessions.get(sessionId))
  }

  /**
   * Close a session with Elastic Search.
   * @param sessionId The session identifier.
   * @return Whether the close command has been issued.
   */
  def close(sessionId: String): Boolean = {
    if (this.Sessions.containsKey(sessionId)) {
      this.Sessions.get(sessionId).close()
      this.Sessions.remove(sessionId)
      true
    } else {
      false
    }
  }
}
