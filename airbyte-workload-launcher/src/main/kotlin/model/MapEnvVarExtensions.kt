/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.model

import io.fabric8.kubernetes.api.model.EnvVar
import io.fabric8.kubernetes.api.model.EnvVarSource

/**
 * For EnvVar that directly contain their values.
 */
private fun Map.Entry<String, String>.toEnvVar() = EnvVar(this.key, this.value, null)

/**
 * For EnvVar that contain a reference to their values (EnvVarSource). Usually secrets.
 */
private fun Map.Entry<String, EnvVarSource>.toRefEnvVar() = EnvVar(this.key, null, this.value)

fun Map<String, String>.toEnvVarList() = this.map { it.toEnvVar() }.toList()

fun Map<String, EnvVarSource>.toRefEnvVarList() = this.map { it.toRefEnvVar() }.toList()
