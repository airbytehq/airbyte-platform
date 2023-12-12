/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.config.Configs.DeploymentMode;
import io.airbyte.config.TolerationPOJO;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import io.micronaut.context.env.Environment;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Micronaut bean factory for worker configuration-related singletons.
 */
@Factory
@Slf4j
@SuppressWarnings({"PMD.AvoidDuplicateLiterals", "LineLength"})
public class ConfigurationBeanFactory {

  private static final String AIRBYTE_ROLE = "AIRBYTE_ROLE";
  private static final String AIRBYTE_VERSION = "AIRBYTE_VERSION";
  private static final String DEPLOYMENT_MODE = "DEPLOYMENT_MODE";
  private static final String DOCKER = "DOCKER";
  private static final String JOB_DEFAULT_ENV_PREFIX = "JOB_DEFAULT_ENV_";
  private static final String KUBERNETES = "KUBERNETES";

  @Singleton
  @Named("jobDefaultEnvMap")
  public Map<String, String> jobDefaultEnvMap(
                                              @Value("${airbyte.role}") final String airbyteRole,
                                              @Value("${airbyte.version}") final String airbyteVersion,
                                              final DeploymentMode deploymentMode,
                                              final Environment environment) {
    final Map<String, String> envMap = System.getenv();
    final Map<String, String> jobPrefixedEnvMap = envMap.keySet().stream()
        .filter(key -> key.startsWith(JOB_DEFAULT_ENV_PREFIX))
        .collect(Collectors.toMap(key -> key.replace(JOB_DEFAULT_ENV_PREFIX, ""), envMap::get));
    final Map<String, String> jobSharedEnvMap = Map.of(AIRBYTE_ROLE, airbyteRole,
        AIRBYTE_VERSION, airbyteVersion,
        DEPLOYMENT_MODE, deploymentMode.name(),
        WorkerConstants.WORKER_ENVIRONMENT, environment.getActiveNames().contains(Environment.KUBERNETES) ? KUBERNETES : DOCKER);
    return MoreMaps.merge(jobPrefixedEnvMap, jobSharedEnvMap);
  }

  /**
   * Returns worker pod tolerations parsed from its own environment variable. The value of the env is
   * a string that represents one or more tolerations.
   * <ul>
   * <li>Tolerations are separated by a `;`
   * <li>Each toleration contains k=v pairs mentioning some/all of key, effect, operator and value and
   * separated by `,`
   * </ul>
   * <p>
   * For example:- The following represents two tolerations, one checking existence and another
   * matching a value
   * <p>
   * key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule
   *
   * @return list of WorkerKubeToleration parsed from env
   */
  @Singleton
  public List<TolerationPOJO> jobKubeTolerations(@Value("${airbyte.worker.job.kube.tolerations}") final String jobKubeTolerations) {
    final Stream<String> tolerations = Strings.isNullOrEmpty(jobKubeTolerations) ? Stream.of()
        : Splitter.on(";")
            .splitToStream(jobKubeTolerations)
            .filter(tolerationStr -> !Strings.isNullOrEmpty(tolerationStr));

    return tolerations
        .map(this::parseToleration)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private TolerationPOJO parseToleration(final String tolerationStr) {
    final Map<String, String> tolerationMap = Splitter.on(",")
        .splitToStream(tolerationStr)
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    if (tolerationMap.containsKey("key") && tolerationMap.containsKey("effect") && tolerationMap.containsKey("operator")) {
      return new TolerationPOJO(
          tolerationMap.get("key"),
          tolerationMap.get("effect"),
          tolerationMap.get("value"),
          tolerationMap.get("operator"));
    } else {
      log.warn(
          "Ignoring toleration {}, missing one of key,effect or operator",
          tolerationStr);
      return null;
    }
  }

}
