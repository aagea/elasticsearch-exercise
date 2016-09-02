package es.alvaroagea.es.exercise.provider

import java.time.Instant
import java.util.Comparator
import java.util.{ArrayList => JArrayList}
import java.util.{HashMap => JHashMap}
import java.util.{List => JList}
import java.util.{Map => JMap}

import es.alvaroagea.es.exercise.manager.AbstractElasticSearchProvider
import es.alvaroagea.es.exercise.provider.ElasticSearchEventProvider.Log
import es.alvaroagea.es.exercise.utils.JavaCollectionsConversions.scalaToJavaConsumer
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ElasticSearchEventProvider {
  val Log: Logger = LoggerFactory.getLogger(classOf[ElasticSearchEventProvider])
  val DocType: String = "event"
  val IdParam: String = "id"
  val TagParam: String = "tag"
  val T1Param: String = "t1"
  val T2Param: String = "t2"

}

class ElasticSearchEventProvider(sessionId: String, index: String)
  extends AbstractElasticSearchProvider(sessionId, index) with EventProvider {

  /**
   * Function that orders to events in ascending order.
   * @param ev1 The first event.
   * @param ev2 The second event.
   * @return A value of the comparison.
   */
  private def ascendingOrder(ev1: Event, ev2: Event): Int = {
    ev1.t1.compareTo(ev2.t1)
  }

  private def hitToEvent(hit: SearchHit): Event = {
    val fields = hit.getFields
    Event(
      fields.get(ElasticSearchEventProvider.IdParam).getValue[String],
      fields.get(ElasticSearchEventProvider.TagParam).getValue[String],
      Instant.ofEpochMilli(fields.get(ElasticSearchEventProvider.T1Param).getValue[Long]),
      Instant.ofEpochMilli(fields.get(ElasticSearchEventProvider.T2Param).getValue[Long])
    )
  }

  private def eventToMap(event: Event): JMap[String, Any] = {
    val result = new JHashMap[String, Any]()
    result.put(ElasticSearchEventProvider.IdParam, event.id)
    result.put(ElasticSearchEventProvider.TagParam, event.tag)
    result.put(ElasticSearchEventProvider.T1Param, event.t1.toEpochMilli)
    result.put(ElasticSearchEventProvider.T2Param, event.t2.toEpochMilli)
    result
  }


  /**
   * The last n occurrences.
   * @param id Id of the stream.
   * @param limit Query limit.
   * @return The list of events.
   */
  override def last(id: String, limit: Int): Option[JList[Event]] = {

    val qb = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery(ElasticSearchEventProvider.IdParam, id))
    val request = this.client.get.prepareSearch(this.index)
      .setTypes(ElasticSearchEventProvider.DocType)
      .addSort(ElasticSearchEventProvider.T1Param, SortOrder.DESC)
      .setQuery(qb)
      .setSize(limit).addFields(
      ElasticSearchEventProvider.IdParam,
      ElasticSearchEventProvider.TagParam,
      ElasticSearchEventProvider.T1Param,
      ElasticSearchEventProvider.T2Param
    )

    Try {
      request.get()
    } match {
      case Success(response: SearchResponse) => {
        if (RestStatus.OK.equals(response.status())) {
          val result = new JArrayList[Event]()
          response.getHits.forEach((hit: SearchHit) => {
            result.add(this.hitToEvent(hit))
          })
          result.sort(new Comparator[Event] {
            override def compare(o1: Event, o2: Event): Int = {
              ascendingOrder(o1, o2)
            }
          })
          Some(result)
        } else {

          None
        }
      }
      case Failure(error) => {
        Log.error("Cannot execute this query: " + request.toString, error)
        None
      }
    }
  }

  /**
   * Take the last occurrences of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param before Before of this date.
   * @return The list of events.
   */
  override def last(id: String, tags: Array[String], limit: Int,
    before: Option[Instant]): Option[JList[Event]] = ???

  /**
   * Take the last distinct occurrences  of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param before Before of this date.
   * @return The list of events.
   */
  override def lastDistinct(id: String, tags: Array[String], limit: Int,
    before: Option[Instant]): Option[JList[Event]] = ???

  /**
   * Take the list of the distinct occurrences between two dates.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param before Init time.
   * @param after After time.
   * @param limit Query limit.
   * @return The list of events.
   */
  override def searchDistinct(id: String, tags: Array[String], before: Instant,
    after: Instant, limit: Int): Option[JList[Event]] = ???

  /**
   * Persist a event in the storage.
   * @param event The selected event.
   * @return The event.
   */
  override def persist(event: Event): Option[Event] = {
    val response = this.client.get
      .prepareIndex(this.index, ElasticSearchEventProvider.DocType)
      .setSource(this.eventToMap(event)).get()

    if (response.isCreated) {
      Some(event)
    } else {
      None
    }
  }

  /**
   * Take the last distinct occurrences  of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param after Before of this date.
   * @return The list of events.
   */
  override def firstDistinct(id: String, tags: Array[String], limit: Int,
    after: Option[Instant]): Option[JList[Event]] = ???

  /**
   * Take the list of the occurrences between two dates.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param before Init time.
   * @param after After time.
   * @param limit Query limit.
   * @return The list of events.
   */
  override def search(id: String, tags: Array[String], before: Instant,
    after: Instant, limit: Int): Option[JList[Event]] = ???

  /**
   * Take the first occurrences of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param after After of this date.
   * @return The list of events.
   */
  override def first(id: String, tags: Array[String], limit: Int,
    after: Option[Instant]): Option[JList[Event]] = ???
}
