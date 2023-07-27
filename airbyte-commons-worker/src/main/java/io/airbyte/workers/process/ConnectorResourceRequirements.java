/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import io.airbyte.config.ResourceRequirements;

/**
 * Defines the resource requirements for a connector.
 * <p>
 * The ProcessFactory interface is currently setting some undesired constraints on how we declare
 * resource requirements. The interface being shared across docker and k8s, it is leading us to
 * expose some k8s complexity to docker. In the case of docker, the only resource requirements
 * needed is the main container, yet we have to expose the rest.
 *
 * @param main resource requirements for the main container
 * @param heartbeat resource requirements for the heartbeat container if applicable
 * @param stdErr resource requirements for the stderr container if applicable
 * @param stdIn resource requirements for the stdin container if applicable
 * @param stdOut resource requirements for the stdout container if applicable
 */
public record ConnectorResourceRequirements(ResourceRequirements main,
                                            ResourceRequirements heartbeat,
                                            ResourceRequirements stdErr,
                                            ResourceRequirements stdIn,
                                            ResourceRequirements stdOut) {

}
