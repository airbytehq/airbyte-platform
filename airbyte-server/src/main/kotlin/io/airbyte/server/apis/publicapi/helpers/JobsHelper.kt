package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.model.generated.JobListForWorkspacesRequestBody
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.server.errors.problems.BadRequestProblem

fun orderByToFieldAndMethod(
  orderBy: String?,
): Pair<JobListForWorkspacesRequestBody.OrderByFieldEnum, JobListForWorkspacesRequestBody.OrderByMethodEnum> {
  var field: JobListForWorkspacesRequestBody.OrderByFieldEnum = JobListForWorkspacesRequestBody.OrderByFieldEnum.CREATEDAT
  var method: JobListForWorkspacesRequestBody.OrderByMethodEnum = JobListForWorkspacesRequestBody.OrderByMethodEnum.ASC
  if (orderBy != null) {
    val pattern: java.util.regex.Pattern = java.util.regex.Pattern.compile("([a-zA-Z0-9]+)\\|(ASC|DESC)")
    val matcher: java.util.regex.Matcher = pattern.matcher(orderBy)
    if (!matcher.find()) {
      throw BadRequestProblem("Invalid order by clause provided: $orderBy")
    }
    field =
      Enums.toEnum(matcher.group(1), JobListForWorkspacesRequestBody.OrderByFieldEnum::class.java)
        .orElseThrow { BadRequestProblem("Invalid order by clause provided: $orderBy") }
    method =
      Enums.toEnum(matcher.group(2), JobListForWorkspacesRequestBody.OrderByMethodEnum::class.java)
        .orElseThrow { BadRequestProblem("Invalid order by clause provided: $orderBy") }
  }
  return Pair(field, method)
}
