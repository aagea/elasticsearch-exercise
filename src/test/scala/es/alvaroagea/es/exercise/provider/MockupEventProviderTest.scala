package es.alvaroagea.es.exercise.provider

class MockupEventProviderTest extends EventProviderBase {
  override val provider: EventProvider = new MockupEventProvider
  this.init()
}
