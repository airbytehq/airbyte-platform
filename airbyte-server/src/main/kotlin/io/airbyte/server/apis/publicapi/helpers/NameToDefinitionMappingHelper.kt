/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.server.apis.publicapi.problems.UnknownValueProblem
import java.util.UUID

fun getActorDefinitionIdFromActorName(
  nameToDefinitionIdMap: Map<String, UUID>,
  name: String?,
): UUID {
  return nameToDefinitionIdMap[name] ?: throw UnknownValueProblem(name)
}
