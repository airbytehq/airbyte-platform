/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

/**
 * Defines any additional tasks that should be executed after successful bootstrapping of the Airbyte
 * environment.
 */
interface PostLoadExecutor {
  /**
   * Executes the additional post bootstrapping tasks.
   *
   * @throws Exception if unable to perform the additional tasks.
   */
  @Throws(Exception::class)
  fun execute()
}
