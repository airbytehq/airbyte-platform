/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod;

/**
 * POJO for Kube container info.
 *
 * @param image image
 * @param pullPolicy pull policy
 */
public record KubeContainerInfo(String image, String pullPolicy) {}
