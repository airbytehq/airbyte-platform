/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations;

import io.airbyte.commons.version.Version;

/**
 * Migration for Protocol Version.
 */
public interface Migration {

  /**
   * The Old version, note that due to semver, the important piece of information is the Major.
   */
  Version getPreviousVersion();

  /**
   * The New version, note that due to semver, the important piece of information is the Major.
   */
  Version getCurrentVersion();

}
