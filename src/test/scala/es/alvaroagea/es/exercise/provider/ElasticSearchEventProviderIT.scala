package es.alvaroagea.es.exercise.provider

import es.alvaroagea.es.exercise.tests.AbstractElasticSearchManagerIT

object ElasticSearchEventProviderIT {
  val TargetIndex: String = "event-provider-it"
}

class ElasticSearchEventProviderIT
  extends AbstractElasticSearchManagerIT with EventProviderBase {

  override val provider: EventProvider =
    new ElasticSearchEventProvider(AbstractElasticSearchManagerIT.TargetSession,
      ElasticSearchEventProviderIT.TargetIndex
    )
  this.removeIndex(ElasticSearchEventProviderIT.TargetIndex)
  this.createIndex(ElasticSearchEventProviderIT.TargetIndex)
  this.init()
  this.refresh(ElasticSearchEventProviderIT.TargetIndex)

}