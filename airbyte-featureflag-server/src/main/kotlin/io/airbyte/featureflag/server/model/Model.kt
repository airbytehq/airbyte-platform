/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag.server.model

data class Context(
  val kind: String,
  val value: String,
)

data class Rule(
  val context: Context,
  val value: String,
)

data class FeatureFlag(
  val key: String,
  val default: String,
  val rules: List<Rule> = listOf(),
)
