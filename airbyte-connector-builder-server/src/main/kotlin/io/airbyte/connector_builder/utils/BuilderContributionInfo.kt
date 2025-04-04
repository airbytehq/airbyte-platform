/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

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
