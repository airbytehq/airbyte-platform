package io.airbyte.data.services.shared

data class ConnectorUpdate(
  val fromVersion: String,
  val toVersion: String,
  val connectorType: ConnectorType,
  val connectorName: String,
  val triggeredBy: String,
) : ConnectionEvent {
  enum class ConnectorType {
    SOURCE,
    DESTINATION,
  }

  enum class UpdateType {
    BREAKING_CHANGE_MANUAL,
  }

  override fun getEventType(): ConnectionEvent.Type {
    return ConnectionEvent.Type.CONNECTOR_UPDATE
  }
}
