/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

/**
 * Kube pod info.
 *
 * @param namespace namespace
 * @param name pod name
 * @param mainContainerInfo container info
 */
public record KubePodInfo(String namespace, String name, KubeContainerInfo mainContainerInfo) {}
