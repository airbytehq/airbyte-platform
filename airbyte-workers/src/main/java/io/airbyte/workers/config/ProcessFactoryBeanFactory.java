/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.workers.process.DockerProcessFactory;
import io.airbyte.workers.process.KubeProcessFactory;
import io.airbyte.workers.process.ProcessFactory;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import io.micronaut.core.util.StringUtils;
import jakarta.inject.Singleton;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;

/**
 * Micronaut bean factory for process factory-related singletons.
 */
@Factory
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ProcessFactoryBeanFactory {

  @Singleton
  @Requires(notEnv = Environment.KUBERNETES)
  public ProcessFactory createDockerProcessFactory(final WorkerConfigsProvider workerConfigsProvider,
                                                   @Value("${docker.network}") final String dockerNetwork,
                                                   @Value("${airbyte.local.docker-mount}") final String localDockerMount,
                                                   @Value("${airbyte.local.root}") final String localRoot,
                                                   @Value("${airbyte.workspace.docker-mount}") final String workspaceDockerMount,
                                                   @Value("${airbyte.workspace.root}") final String workspaceRoot) {
    return new DockerProcessFactory(
        workerConfigsProvider,
        Path.of(workspaceRoot),
        StringUtils.isNotEmpty(workspaceDockerMount) ? workspaceDockerMount : workspaceRoot,
        StringUtils.isNotEmpty(localDockerMount) ? localDockerMount : localRoot,
        dockerNetwork);
  }

  @Singleton
  @Requires(env = Environment.KUBERNETES)
  public ProcessFactory createKubernetesProcessFactory(final WorkerConfigsProvider workerConfigsProvider,
                                                       final FeatureFlagClient featureFlagClient,
                                                       @Value("${airbyte.worker.job.kube.namespace}") final String kubernetesNamespace,
                                                       @Value("${airbyte.worker.job.kube.serviceAccount}") final String serviceAccount,
                                                       @Value("${micronaut.server.port}") final Integer serverPort)
      throws UnknownHostException {
    final KubernetesClient fabricClient = new DefaultKubernetesClient();
    final String localIp = InetAddress.getLocalHost().getHostAddress();
    final String kubeHeartbeatUrl = localIp + ":" + serverPort;
    return new KubeProcessFactory(workerConfigsProvider,
        featureFlagClient,
        kubernetesNamespace,
        serviceAccount,
        fabricClient,
        kubeHeartbeatUrl);
  }

}
