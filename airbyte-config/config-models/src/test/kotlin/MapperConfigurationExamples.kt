import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.SyncMode

/**
 * Sample of ConfiguredAirbyteStream with mapper configurations.
 */
val configuredUsersStream =
  ConfiguredAirbyteStream(
    stream =
      AirbyteStream(
        name = "users",
        jsonSchema = Jsons.emptyObject(),
        supportedSyncModes = listOf(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL),
      ),
    syncMode = SyncMode.FULL_REFRESH,
    destinationSyncMode = DestinationSyncMode.OVERWRITE,
    primaryKey = listOf(listOf("id")),
    fields =
      listOf(
        Field(name = "id", type = FieldType.NUMBER),
        Field(name = "name", type = FieldType.STRING),
        Field(name = "email", type = FieldType.STRING),
        Field(name = "secret", type = FieldType.STRING),
      ),
    mappers =
      listOf(
        ConfiguredMapper(
          name = "hash",
          config =
            mapOf(
              "field" to "email",
              "type" to "sha1",
            ),
        ),
        ConfiguredMapper(
          name = "encrypt",
          config =
            mapOf(
              "field" to "secret",
              "type" to "rot13",
            ),
        ),
      ),
  )
