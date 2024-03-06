/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.problems

import io.airbyte.commons.server.errors.problems.AbstractThrowableProblem
import io.airbyte.public_api.model.generated.ConnectionSyncModeEnum
import io.airbyte.server.apis.publicapi.constants.API_DOC_URL
import io.micronaut.http.HttpStatus
import java.io.Serial
import java.net.URI
import javax.validation.Valid

/**
 * Thrown when a configuration for a connection is not valid.
 */
class ConnectionConfigurationProblem private constructor(message: String) : AbstractThrowableProblem(
  io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem.Companion.TYPE,
  io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem.Companion.TITLE,
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
    ): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "Cannot set sync mode to $connectionSyncMode for stream $streamName. Valid sync modes are: $validSyncModes",
      )
    }

    fun invalidStreamName(validStreamNames: Collection<String?>): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "Invalid stream found. The list of valid streams include: $validStreamNames.",
      )
    }

    fun duplicateStream(streamName: String): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem("Duplicate stream found in configuration for: $streamName.")
    }

    fun sourceDefinedCursorFieldProblem(
      streamName: String,
      cursorField: List<String?>,
    ): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "Cursor Field " + cursorField + " is already defined by source for stream: " + streamName +
          ". Do not include a cursor field configuration for this stream.",
      )
    }

    fun missingCursorField(streamName: String): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "No default cursor field for stream: $streamName. Please include a cursor field configuration for this stream.",
      )
    }

    fun invalidCursorField(
      streamName: String,
      validFields: List<List<String?>?>,
    ): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "Invalid cursor field for stream: $streamName. The list of valid cursor fields include: $validFields.",
      )
    }

    fun missingPrimaryKey(streamName: String): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "No default primary key for stream: $streamName. Please include a primary key configuration for this stream.",
      )
    }

    fun primaryKeyAlreadyDefined(streamName: String): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "Primary key for stream: $streamName is already pre-defined. Please do NOT include a primary key configuration for this stream.",
      )
    }

    fun invalidPrimaryKey(
      streamName: String,
      validFields: List<List<String?>?>,
    ): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "Invalid cursor field for stream: $streamName. The list of valid primary keys fields: $validFields.",
      )
    }

    fun invalidCronExpressionUnderOneHour(cronExpression: String): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "The cron expression " + cronExpression +
          " is not valid or is less than the one hour minimum. The seconds and minutes values cannot be `*`.",
      )
    }

    fun invalidCronExpression(
      cronExpression: String,
      message: String?,
    ): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem(
        "The cron expression $cronExpression is not valid. Error: $message" +
          ". Please check the cron expression format at https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html",
      )
    }

    fun missingCronExpression(): io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem {
      return io.airbyte.server.apis.publicapi.problems.ConnectionConfigurationProblem("Missing cron expression in the schedule.")
    }
  }
}
