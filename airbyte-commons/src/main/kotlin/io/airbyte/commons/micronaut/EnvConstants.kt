/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.micronaut

/**
 * Internal micronaut environment strings used for triggering conditional property or bean loading.
 *
 * Amends the default available environment strings defined in io.micronaut.context.env.Environment.
 */
object EnvConstants {
  const val DATA_PLANE = "data-plane"
  const val CONTROL_PLANE = "control-plane"
  const val LOCAL_TEST = "local-test"
  const val WORKER_V2 = "worker-v2"
  const val LOCAL_SECRETS = "local-secrets"
}
