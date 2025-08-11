/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.contributions

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

data class BuilderContributionInfo(
  val isEdit: Boolean,
  val connectorName: String,
  val connectorImageName: String,
  val actorDefinitionId: String,
  val connectorDescription: String,
  val contributionDescription: String,
  val githubToken: String,
  val manifestYaml: String,
  val customComponents: String?, // optional
  val baseImage: String,
  val versionTag: String,
  val authorUsername: String,
  val changelogMessage: String,
  val updateDate: String = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDate.now()),
)

data class ContributionCreate(
  val name: String,
  val connectorImageName: String,
  val connectorDescription: String,
  val githubToken: String,
  val manifestYaml: String,
  val baseImage: String,
  val contributionDescription: String,
  val customComponents: String? = null,
)

data class ContributionCreateResult(
  val pullRequestUrl: String,
  val actorDefinitionId: UUID,
)

data class ContributionRead(
  val connectorExists: Boolean,
  val connectorName: String? = null,
  val connectorDescription: String? = null,
  val githubUrl: String? = null,
)
