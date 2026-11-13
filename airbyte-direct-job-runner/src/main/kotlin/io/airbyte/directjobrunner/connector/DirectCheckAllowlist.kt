/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import io.airbyte.directjobrunner.core.LauncherDataplaneGroup
import io.airbyte.featureflag.DataplaneGroup
import io.airbyte.featureflag.DirectCheckImages
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.isListedAirbyteConnector
import io.airbyte.featureflag.parseDirectCheckConnectors
import jakarta.inject.Singleton

/**
 * The connectors (`source-postgres`) whose checks the runner takes, at whatever version the check asks for: the runner
 * runs the jar published for that version. The list is `platform.direct-check-images` for the dispatching launcher's
 * data plane group, the same flag and context the launcher routed the check with. A check for any other image, or from
 * a launcher that sent no group, is turned down and runs in a pod.
 */
@Singleton
class DirectCheckAllowlist(
  private val featureFlagClient: FeatureFlagClient,
) {
  /** The listed connectors, read from the flag on every call so a change applies without a restart. */
  fun connectors(dataplaneGroup: LauncherDataplaneGroup): Set<String> =
    when (dataplaneGroup) {
      is LauncherDataplaneGroup.Known ->
        parseDirectCheckConnectors(featureFlagClient.stringVariation(DirectCheckImages, DataplaneGroup(dataplaneGroup.dataplaneGroupId)))
      LauncherDataplaneGroup.Unknown -> emptySet()
    }

  /** Whether [image] is Airbyte's image of a connector listed for [dataplaneGroup] ([isListedAirbyteConnector]). */
  fun allows(
    image: String,
    dataplaneGroup: LauncherDataplaneGroup,
  ): Boolean = isListedAirbyteConnector(image, connectors(dataplaneGroup))
}
