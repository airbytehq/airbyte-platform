package io.airbyte.commons.server.errors.problems

import io.airbyte.commons.server.API_DOC_URL
import io.airbyte.public_api.model.generated.ConnectionSyncModeEnum
import io.micronaut.http.HttpStatus
import jakarta.validation.Valid
import java.io.Serial
import java.net.URI

/**
 * Thrown when a configuration for a connection is not valid.
 */
class ConnectionConfigurationProblem private constructor(message: String) : AbstractThrowableProblem(
  TYPE,
  TITLE,
  HttpStatus.BAD_REQUEST,
  "The body of the request contains an invalid connection configuration. $message",
) {
  companion object {
    @Serial
    private val serialVersionUID = 1L
    private val TYPE = URI.create("$API_DOC_URL/reference/errors")
    private const val TITLE = "bad-request"

    fun handleSyncModeProblem(
      connectionSyncMode: @Valid ConnectionSyncModeEnum?,
      streamName: String,
      validSyncModes: Set<ConnectionSyncModeEnum?>,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Cannot set sync mode to $connectionSyncMode for stream $streamName. Valid sync modes are: $validSyncModes",
      )
    }

    fun invalidStreamName(validStreamNames: Collection<String?>): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Invalid stream found. The list of valid streams include: $validStreamNames.",
      )
    }

    fun duplicateStream(streamName: String): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem("Duplicate stream found in configuration for: $streamName.")
    }

    fun sourceDefinedCursorFieldProblem(
      streamName: String,
      cursorField: List<String?>,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Cursor Field " + cursorField + " is already defined by source for stream: " + streamName +
          ". Do not include a cursor field configuration for this stream.",
      )
    }

    fun missingCursorField(streamName: String): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "No default cursor field for stream: $streamName. Please include a cursor field configuration for this stream.",
      )
    }

    fun invalidCursorField(
      streamName: String,
      validFields: List<List<String?>?>,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Invalid cursor field for stream: $streamName. The list of valid cursor fields include: $validFields.",
      )
    }

    fun missingPrimaryKey(streamName: String): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "No default primary key for stream: $streamName. Please include a primary key configuration for this stream.",
      )
    }

    fun primaryKeyAlreadyDefined(
      streamName: String,
      allowedPrimaryKey: List<List<String>>,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Primary key for stream: $streamName is already pre-defined. Please remove the primaryKey or provide the value as $allowedPrimaryKey.",
      )
    }

    fun invalidPrimaryKey(
      streamName: String,
      validFields: List<List<String?>?>,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Invalid cursor field for stream: $streamName. The list of valid primary keys fields: $validFields.",
      )
    }

    fun duplicatePrimaryKey(
      streamName: String,
      key: List<List<String?>?>,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "Duplicate primary key detected for stream: $streamName, please don't provide the same column more than once. Key: $key",
      )
    }

    fun invalidCronExpressionUnderOneHour(cronExpression: String): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "The cron expression " + cronExpression +
          " is not valid or is less than the one hour minimum. The seconds and minutes values cannot be `*`.",
      )
    }

    fun invalidCronExpression(
      cronExpression: String,
      message: String?,
    ): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem(
        "The cron expression $cronExpression is not valid. Error: $message" +
          ". Please check the cron expression format at https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html",
      )
    }

    fun missingCronExpression(): ConnectionConfigurationProblem {
      return ConnectionConfigurationProblem("Missing cron expression in the schedule.")
    }
  }
}
