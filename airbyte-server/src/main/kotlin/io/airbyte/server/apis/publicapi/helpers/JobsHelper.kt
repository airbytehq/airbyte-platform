/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.models.JobListForWorkspacesRequestBody
import io.airbyte.commons.enums.toEnum

fun orderByToFieldAndMethod(orderBy: String?): Pair<JobListForWorkspacesRequestBody.OrderByField, JobListForWorkspacesRequestBody.OrderByMethod> {
  var field: JobListForWorkspacesRequestBody.OrderByField = JobListForWorkspacesRequestBody.OrderByField.CREATED_AT
  var method: JobListForWorkspacesRequestBody.OrderByMethod = JobListForWorkspacesRequestBody.OrderByMethod.ASC
  if (orderBy != null) {
    val pattern: java.util.regex.Pattern =
      java.util.regex.Pattern
        .compile("([a-zA-Z0-9]+)\\|(ASC|DESC)")
    val matcher: java.util.regex.Matcher = pattern.matcher(orderBy)
    if (!matcher.find()) {
      throw BadRequestProblem(ProblemMessageData().message("Invalid order by clause provided: $orderBy"))
    }
    field =
      matcher.group(1).toEnum<JobListForWorkspacesRequestBody.OrderByField>()
        ?: throw BadRequestProblem(ProblemMessageData().message("Invalid order by clause provided: $orderBy"))
    method =
      matcher.group(2).toEnum<JobListForWorkspacesRequestBody.OrderByMethod>()
        ?: throw BadRequestProblem(ProblemMessageData().message("Invalid order by clause provided: $orderBy"))
  }
  return Pair(field, method)
}
