/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job;

import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * General persistence interface for storing metadata.
 */
public interface MetadataPersistence {

  /**
   * Returns the AirbyteVersion.
   */
  Optional<String> getVersion() throws IOException;

  /**
   * Set the airbyte version.
   */
  void setVersion(String airbyteVersion) throws IOException;

  /**
   * Get the max supported Airbyte Protocol Version.
   */
  Optional<Version> getAirbyteProtocolVersionMax() throws IOException;

  /**
   * Set the max supported Airbyte Protocol Version.
   */
  void setAirbyteProtocolVersionMax(Version version) throws IOException;

  /**
   * Get the min supported Airbyte Protocol Version.
   */
  Optional<Version> getAirbyteProtocolVersionMin() throws IOException;

  /**
   * Set the min supported Airbyte Protocol Version.
   */
  void setAirbyteProtocolVersionMin(Version version) throws IOException;

  /**
   * Get the current Airbyte Protocol Version range if defined.
   */
  Optional<AirbyteProtocolVersionRange> getCurrentProtocolVersionRange() throws IOException;

  /**
   * Returns a deployment UUID.
   */
  Optional<UUID> getDeployment() throws IOException;
  // a deployment references a setup of airbyte. it is created the first time the docker compose or
  // K8s is ready.

  /**
   * Set deployment id. If one is already set, the new value is ignored.
   */
  void setDeployment(UUID uuid) throws IOException;

}
