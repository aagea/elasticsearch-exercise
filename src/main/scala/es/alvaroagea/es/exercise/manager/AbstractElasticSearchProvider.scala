package es.alvaroagea.es.exercise.manager

import org.elasticsearch.client.transport.TransportClient

/**
 * Abstract ElasticSearch provider.
 */
abstract class AbstractElasticSearchProvider(sessionId: String, index: String) {

  /**
   * Client used to connect to Elastic Search.
   */
  val client : Option[TransportClient] = ElasticSearchSessionManager.getSession(this.sessionId)

}
