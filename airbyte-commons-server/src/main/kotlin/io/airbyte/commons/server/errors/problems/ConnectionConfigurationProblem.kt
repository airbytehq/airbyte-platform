package io.airbyte.commons.server.errors.problems

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.public_api.model.generated.ConnectionSyncModeEnum
import jakarta.validation.Valid

/**
 * Thrown when a configuration for a connection is not valid.
 * These were created before standardizing our approach of throwing Problems and should be turned into specific problem types.
 */
@Deprecated("Create a specific problem types for each case instead.")
class ConnectionConfigurationProblem private constructor() {
  companion object {
    fun handleSyncModeProblem(
      connectionSyncMode: @Valid ConnectionSyncModeEnum?,
      streamName: String,
      validSyncModes: Set<ConnectionSyncModeEnum?>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Cannot set sync mode to $connectionSyncMode for stream $streamName. Valid sync modes are: $validSyncModes",
        ),
      )
    }

    fun invalidStreamName(validStreamNames: Collection<String?>): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Invalid stream found. The list of valid streams include: $validStreamNames.",
        ),
      )
    }

    fun invalidFieldName(
      streamName: String,
      validFieldNames: Collection<String?>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Invalid field selected in configuration for stream $streamName. The list of valid field names includes: $validFieldNames.",
        ),
      )
    }

    fun duplicateStream(streamName: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Duplicate stream found in configuration for: $streamName.",
        ),
      )
    }

    fun duplicateFieldsSelected(streamName: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Duplicate fields selected in configuration for stream: $streamName.",
        ),
      )
    }

    fun sourceDefinedCursorFieldProblem(
      streamName: String,
      cursorField: List<String?>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Cursor Field " + cursorField + " is already defined by source for stream: " + streamName +
            ". Do not include a cursor field configuration for this stream.",
        ),
      )
    }

    fun missingCursorField(streamName: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "No default cursor field for stream: $streamName. Please include a cursor field configuration for this stream.",
        ),
      )
    }

    fun missingCursorFieldSelected(streamName: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Cursor field is not selected properly for stream: $streamName. Please include the cursor field in selected fields for this stream.",
        ),
      )
    }

    fun invalidCursorField(
      streamName: String,
      validFields: List<List<String?>?>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Invalid cursor field for stream: $streamName. The list of valid cursor fields include: $validFields.",
        ),
      )
    }

    fun missingPrimaryKey(streamName: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "No default primary key for stream: $streamName. Please include a primary key configuration for this stream.",
        ),
      )
    }

    fun missingPrimaryKeySelected(streamName: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Primary key fields are not selected properly for stream: $streamName. " +
            "Please include the primary key fields in selected fields for this stream.",
        ),
      )
    }

    fun primaryKeyAlreadyDefined(
      streamName: String,
      allowedPrimaryKey: List<List<String>>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Primary key for stream: $streamName is already pre-defined. Please remove the primaryKey or provide the value as $allowedPrimaryKey.",
        ),
      )
    }

    fun invalidPrimaryKey(
      streamName: String,
      validFields: List<List<String?>?>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Invalid cursor field for stream: $streamName. The list of valid primary keys fields: $validFields.",
        ),
      )
    }

    fun duplicatePrimaryKey(
      streamName: String,
      key: List<List<String?>?>,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "Duplicate primary key detected for stream: $streamName, please don't provide the same column more than once. Key: $key",
        ),
      )
    }

    fun invalidCronExpressionUnderOneHour(cronExpression: String): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "The cron expression " + cronExpression +
            " is not valid or is less than the one hour minimum. The seconds and minutes values cannot be `*`.",
        ),
      )
    }

    fun invalidCronExpression(
      cronExpression: String,
      message: String?,
    ): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message(
          "The cron expression $cronExpression is not valid. Error: $message" +
            ". Please check the cron expression format at https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html",
        ),
      )
    }

    fun missingCronExpression(): BadRequestProblem {
      return BadRequestProblem(
        ProblemMessageData().message("Missing cron expression in the schedule."),
      )
    }
  }
}
