/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.util.Map;

/**
 * Configuration for Container Orchestrator.
 *
 * @param namespace kube namespace
 * @param documentStoreClient document store client
 * @param environmentVariables env variable
 * @param kubernetesClient kube clinet
 * @param secretName secret ?
 * @param secretMountPath secret mount path ?
 * @param dataPlaneCredsSecretName name of where creds to accessing the data plane
 * @param dataPlaneCredsSecretMountPath creds for accessing data plane
 * @param containerOrchestratorImage container orchestrator image
 * @param containerOrchestratorImagePullPolicy container pull policy
 * @param googleApplicationCredentials gcp creds
 * @param workerEnvironment worker env
 * @param serviceAccount kube service account for orchestrator pod
 */
public record ContainerOrchestratorConfig(
                                          String namespace,
                                          DocumentStoreClient documentStoreClient,
                                          Map<String, String> environmentVariables,
                                          KubernetesClient kubernetesClient,
                                          String secretName,
                                          String secretMountPath,
                                          String dataPlaneCredsSecretName,
                                          String dataPlaneCredsSecretMountPath,
                                          String containerOrchestratorImage,
                                          String containerOrchestratorImagePullPolicy,
                                          String googleApplicationCredentials,
                                          WorkerEnvironment workerEnvironment,
                                          String serviceAccount) {}
