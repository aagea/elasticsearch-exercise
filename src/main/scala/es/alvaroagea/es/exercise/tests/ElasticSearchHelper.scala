package es.alvaroagea.es.exercise.tests

import java.net.InetAddress

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object ElasticSearchHelper{

  /**
   * Environment variable to set the Elastic search host.
   */
  val HostEnvironmentVariable : String = "ES_HOST"

  /**
   * Environment variable to set the Elastic search port.
   */
  val PortEnvironmentVariable : String = "ES_PORT"

  /**
   * Default Elastic search port.
   */
  val DefaultPort : Int = 9300

  /**
   * Default host.
   */
  val DefaultHost : String = "localhost"

  /**
   * Class logger.
   */
  private val Log: Logger = LoggerFactory.getLogger(this.getClass.getName)
}

/**
 * Elastic search tests helper.
 * @param host The target host.
 * @param port The target port.
 */
class ElasticSearchHelper(
  host: String = ElasticSearchHelper.DefaultHost,
  port : Int = ElasticSearchHelper.DefaultPort) {

  /**
   * The elastic search client.
   */
  private var client : Option[TransportClient] = None

  /**
   * Connect to Elastic search on the class host and port.
   * @return Whether it is connected or not.
   */
  def connect() : Boolean = {
    val settings : Settings = Settings.settingsBuilder()
      .put("client.transport.sniff", true).build()
    val addr = InetAddress.getByName(this.host)
    this.client = Option(TransportClient.builder().settings(settings).build()
      .addTransportAddress(new InetSocketTransportAddress(addr, this.port)))
    this.isConnected()
  }

  /**
   * Determine whether the connection with Elastic Search is available.
   * @return Whether is linked or not.
   */
  def isConnected() : Boolean = {
    if(client.isDefined){
      client.get.connectedNodes().size() > 0
    }else{
      false
    }
  }

  /**
   * Close the client with Elastic search.
   */
  def close() : Unit = {
    if(this.client.isDefined){
      client.get.close()
    }
  }

  /**
   * Create a new index.
   * @param name The name of the index.
   * @return Whether the index has been created.
   */
  def createIndex(name: String) : Boolean = {
    val created = this.client.get.admin().indices().prepareCreate(name).get()
    created.isAcknowledged
  }

  /**
   * Determine if an index exists.
   * @param name The name of the index.
   * @return Whether it exists or not.
   */
  def indexExists(name: String) : Boolean = {
    val exists = this.client.get.admin().indices().prepareExists(name).get
    exists.isExists
  }

  /**
   * Remove an index from Elastic Search.
   * @param name The name of the index.
   * @return Whether the index has been removed.
   */
  def removeIndex(name: String) : Boolean = {
    val indexExists = this.client.get.admin().indices()
      .exists(new IndicesExistsRequest(name)).actionGet()
    if(indexExists.isExists) {
      val response: DeleteIndexResponse = this.client.get.admin()
        .indices().delete(new DeleteIndexRequest(name)).actionGet()
      response.isAcknowledged
    }else{
      true
    }
  }

  /**
   * Refresh an index.
   * @param name The name of the index.
   * @return Whether the index has been refreshed.
   */
  def refresh(name: String) : Boolean = {
    val response : RefreshResponse = this.client.get.admin()
      .indices().refresh(new RefreshRequest(name)).actionGet()
    response.getSuccessfulShards > 0
  }

  /**
   * Index a document in Elastic Search.
   * @param index The name of the index.
   * @param docType The type of document.
   * @param id The document identifier.
   * @param document The document.
   * @return Whether the document has been indexed.
   */
  def indexDocument(index: String, docType: String, id: String, document: String) : Boolean = {
    val inserted = this.client.get.prepareIndex(index, docType, id).setSource(document).get()
    inserted.isCreated
  }

  /**
   * Get a document.
   * @param index The index name.
   * @param docType The document type.
   * @param id The document identifier.
   * @return An option with the document as a String.
   */
  def getDocument(index: String, docType: String, id: String) : Option[String] = {
    val retrieved = this.client.get.prepareGet(index, docType, id).get()
    if(retrieved.isExists){
      Some(retrieved.getSourceAsString)
    }else{
      None
    }
  }
}