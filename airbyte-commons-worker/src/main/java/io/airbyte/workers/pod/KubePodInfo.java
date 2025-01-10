/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod;

/**
 * Kube pod info.
 *
 * @param namespace namespace
 * @param name pod name
 * @param mainContainerInfo container info
 */
public record KubePodInfo(String namespace, String name, KubeContainerInfo mainContainerInfo) {}
