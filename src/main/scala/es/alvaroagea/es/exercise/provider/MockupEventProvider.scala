package es.alvaroagea.es.exercise.provider

import java.time.Instant
import java.util.stream.Collectors
import java.util.{ArrayList => JArrayList}
import java.util.{List => JList}

import es.alvaroagea.es.exercise.utils.JavaCollectionsConversions.scalaToJavaComparator
import es.alvaroagea.es.exercise.utils.JavaCollectionsConversions.scalaToJavaPredicate

class MockupEventProvider extends EventProvider {

  val events: JList[Event] = new JArrayList[Event]()

  /**
   * Empty filter for passing all window behaviors.
   * @param wb The window behavior to be analyzed.
   * @return True.
   */
  private def emptyFilter(wb: Event): Boolean = {
    true
  }

  /**
   * Before filter on the T1 time.
   * @param before The timestamp that must be after all behaviors T1.
   * @return Whether the events matches the filter.
   */
  private def beforeFilter(before: Instant): (Event) => Boolean = {
    (ev: Event) => ev.t1.isBefore(before)
  }

  /**
   * After filter on the T1 time.
   * @param after The timestamp that must be before all events.
   * @return Whether the events matches the filter.
   */
  private def afterFilter(after: Instant): (Event) => Boolean = {
    (ev: Event) => ev.t1.isAfter(after)
  }

  /**
   * Function that orders to events in ascending order.
   * @param ev1 The first event.
   * @param ev2 The second event.
   * @return A value of the comparison.
   */
  private def ascendingOrder(ev1: Event, ev2: Event): Int = {
    ev1.t1.compareTo(ev2.t1)
  }

  /**
   * Get the behaviors that match the search.
   * @param queryFunction The function to be applied for matching the key.
   * @param limit The number of elements to be retrieved.
   * @param beforeFilter The filter to be applied in case a before clause is used.
   * @param afterFilter The filter to be applied in case an after clause is used.
   * @return The list of matching behaviors.
   */
  private def getEvents(
    queryFunction: Event => Boolean,
    beforeFilter: Event => Boolean,
    afterFilter: Event => Boolean,
    limit: Int,
    filterLast: Boolean = true
  ): Option[JList[Event]] = {

    val result = this.events.stream()
      .filter(beforeFilter)
      .filter(afterFilter)
      .filter(queryFunction)
      .sorted(this.ascendingOrder _)
      .collect(Collectors.toList[Event])
    if (result.size() > limit) {
      if (filterLast) {
        Some(result.subList(result.size() - limit, result.size()))
      } else {
        Some(result.subList(0, limit))
      }
    } else {
      Some(result)
    }
  }


  /**
   * The last n occurrences.
   * @param id Id of the stream.
   * @param limit Query limit.
   * @return The list of events.
   */
  override def last(id: String, limit: Int): Option[JList[Event]] = {
    this.getEvents(
      (ev:Event)=>ev.id==id,
      emptyFilter,
      emptyFilter,
      limit
    )
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
    before: Option[Instant]): Option[JList[Event]] = {
    this.getEvents(
      (ev:Event)=>ev.id==id && tags.contains(ev.tag),
      if (before.isDefined) this.beforeFilter(before.get) else this.emptyFilter,
      this.emptyFilter,
      limit
    )
  }

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
    after: Instant, limit: Int): Option[JList[Event]] = {
    this.getEvents(
      (ev: Event) => ev.id==id && !tags.contains(ev.tag),
      this.beforeFilter(before),
      this.afterFilter(after),
      limit

    )
  }

  /**
   * Take the last distinct occurrences  of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param before Before of this date.
   * @return The list of events.
   */
  override def lastDistinct(id: String, tags: Array[String], limit: Int,
    before: Option[Instant]): Option[JList[Event]] = {
    this.getEvents(
      (ev:Event)=>ev.id==id && !tags.contains(ev.tag),
      if (before.isDefined) this.beforeFilter(before.get) else this.emptyFilter,
      this.emptyFilter,
      limit
    )
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
    after: Option[Instant]): Option[JList[Event]] = {
    this.getEvents(
      (ev:Event)=>ev.id==id && !tags.contains(ev.tag),
      this.emptyFilter,
      if (after.isDefined) this.afterFilter(after.get) else this.emptyFilter,
      limit,
      filterLast = false
    )
  }

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
    after: Instant, limit: Int): Option[JList[Event]] = {
    this.getEvents(
      (ev: Event) => ev.id==id && tags.contains(ev.tag),
      this.beforeFilter(before),
      this.afterFilter(after),
      limit
    )
  }

  /**
   * Take the first occurrences of a list of tags.
   * @param id Id of the stream.
   * @param tags List of tags.
   * @param limit Query limit.
   * @param after After of this date.
   * @return The list of events.
   */
  override def first(id: String, tags: Array[String], limit: Int,
    after: Option[Instant]): Option[JList[Event]] = {
    this.getEvents(
      (ev:Event)=>ev.id==id && tags.contains(ev.tag),
      this.emptyFilter,
      if (after.isDefined) this.afterFilter(after.get) else this.emptyFilter,
      limit,
      filterLast = false
    )
  }

  /**
   * Persist a event in the storage.
   * @param event The selected event.
   * @return The event.
   */
  override def persist(event: Event): Option[Event] = {
    this.events.add(event)
    Some(event)
  }
}
