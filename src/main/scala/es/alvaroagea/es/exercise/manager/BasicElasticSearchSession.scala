package es.alvaroagea.es.exercise.manager

import java.net.InetAddress
import java.util.Arrays
import java.util.List
import java.util.stream.Collectors

import org.elasticsearch.common.transport.InetSocketTransportAddress

import es.alvaroagea.es.exercise.utils.JavaCollectionsConversions.scalaToJavaFunction

object BasicElasticSearchSession{

  /**
   * Elastic Search default port.
   */
  val ElasticSearchDefaultPort : Int = 9300
}

/**
 * Class that contains the information required to establish a basic session with ElasticSearch
 * using the Transport client.
 * @param id The session identifier.
 * @param addresses The list of addresses.
 * @param port The port for Elastic Search transport clients.
 */
case class BasicElasticSearchSession(id: String, addresses: List[String], port: Int) {

  /**
   * Create an Elastic Search session using the default port.
   * @param id The session identifier.
   * @param addresses The list of addresses.
   */
  def this(id: String, addresses: List[String]) = {
    this(id, addresses, BasicElasticSearchSession.ElasticSearchDefaultPort)
  }

  /**
   * Create a Elastic Search session using the default port connecting to a single host.
   * @param id The session identifier.
   * @param host The target host.
   */
  def this(id: String, host: String) = {
    this(id, Arrays.asList(host), BasicElasticSearchSession.ElasticSearchDefaultPort)
  }

  /**
   * Create a Elastic Search session connecting to a single host.
   * @param id The session identifier.
   * @param host The target host.
   * @param port The target port.
   */
  def this(id: String, host: String, port: Int) = {
    this(id, Arrays.asList(host), port)
  }

  /**
   * Transform the current session into a list of transport addresses to build the client.
   * @return A list of transport addresses.
   */
  def asTransportAddresses() : List[InetSocketTransportAddress] = {
    this.addresses.stream().map[InetSocketTransportAddress]((host: String) => {
      new InetSocketTransportAddress(InetAddress.getByName(host), this.port)
    }).collect(Collectors.toList[InetSocketTransportAddress])
  }

}
