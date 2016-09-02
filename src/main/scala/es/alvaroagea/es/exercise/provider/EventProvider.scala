package es.alvaroagea.es.exercise.provider

import java.time.Instant
import java.util.{List => JList}

trait EventProvider {

  /**
   * The last n occurrences.
   * @param id Id of the stream.
   * @param limit Query limit.
   * @return The list of events.
   */
  def last(id: String, limit: Int)
  : Option[JList[Event]]

  /**
   * Take the last occurrences of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param before Before of this date.
   * @return The list of events.
   */
  def last(id: String, tags: Array[String],
    limit: Int, before: Option[Instant] = None): Option[JList[Event]]

  /**
   * Take the last distinct occurrences  of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param before Before of this date.
   * @return The list of events.
   */
  def lastDistinct(id: String, tags: Array[String],
    limit: Int, before: Option[Instant] = None): Option[JList[Event]]

  /**
   * Take the list of the occurrences between two dates.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param before Init time.
   * @param after After time.
   * @param limit Query limit.
   * @return The list of events.
   */
  def search(id: String, tags: Array[String],
    before: Instant, after: Instant, limit: Int): Option[JList[Event]]

  /**
   * Take the list of the distinct occurrences between two dates.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param before Init time.
   * @param after After time.
   * @param limit Query limit.
   * @return The list of events.
   */
  def searchDistinct(id: String, tags: Array[String],
    before: Instant, after: Instant, limit: Int): Option[JList[Event]]

  /**
   * Take the first occurrences of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param after After of this date.
   * @return The list of events.
   */
  def first(id: String, tags: Array[String],
    limit: Int, after: Option[Instant] = None): Option[JList[Event]]

  /**
   * Take the last distinct occurrences  of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param after Before of this date.
   * @return The list of events.
   */
  def firstDistinct(id: String, tags: Array[String],
    limit: Int, after: Option[Instant] = None): Option[JList[Event]]

  /**
   * Persist a event in the storage.
   * @param event The selected event.
   * @return The event.
   */
  def persist(event: Event): Option[Event]


}
