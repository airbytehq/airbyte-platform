/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.fusion

import java.util.UUID

data class SkillDocsActor(
  val definitionId: UUID,
  val name: String,
)

data class SkillDocsSource(
  val sourceId: UUID,
  val name: String,
  val definitionName: String,
)
