/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.commons.json.Jsons;
import io.airbyte.workers.pod.FileConstants;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * The job orchestrator helps abstract over container launcher application differences.
 *
 * @param <INPUT> job input type
 */
public interface JobOrchestrator<INPUT> {

  // used to serialize the loaded input
  Class<INPUT> getInputClass();

  default Path getConfigDir() {
    return Path.of(FileConstants.CONFIG_DIR);
  }

  /**
   * Reads input from a file that was copied to the container launcher.
   *
   * @return input
   * @throws IOException when communicating with container
   */
  default INPUT readInput() throws IOException {
    return Jsons.deserialize(
        getConfigDir().resolve(FileConstants.INIT_INPUT_FILE).toFile(),
        getInputClass());
  }

  /**
   * Contains the unique logic that belongs to each type of job.
   *
   * @return an optional output value to place within the output document store item.
   */
  Optional<String> runJob() throws Exception;

  static String workloadId() {
    return EnvVar.WORKLOAD_ID.fetch();
  }

}
