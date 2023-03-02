/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

/**
 * POJO for Kube container info.
 *
 * @param image image
 * @param pullPolicy pull policy
 */
public record KubeContainerInfo(String image, String pullPolicy) {}
