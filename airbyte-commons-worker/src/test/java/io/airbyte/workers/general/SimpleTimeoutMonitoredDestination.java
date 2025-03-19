/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.internal.DestinationTimeoutMonitor;
import io.airbyte.workers.internal.SimpleAirbyteDestination;

/**
 * This class only exists to test the timeout monitor.
 */
public class SimpleTimeoutMonitoredDestination extends SimpleAirbyteDestination {

  private final DestinationTimeoutMonitor destinationTimeoutMonitor;

  public SimpleTimeoutMonitoredDestination(DestinationTimeoutMonitor destinationTimeoutMonitor) {
    this.destinationTimeoutMonitor = destinationTimeoutMonitor;
  }

  @Override
  public void accept(AirbyteMessage message) throws Exception {
    destinationTimeoutMonitor.startAcceptTimer();
    super.accept(message);
    destinationTimeoutMonitor.resetAcceptTimer();
  }

  @Override
  public void notifyEndOfInput() throws Exception {
    destinationTimeoutMonitor.startNotifyEndOfInputTimer();
    super.notifyEndOfInput();
    destinationTimeoutMonitor.resetNotifyEndOfInputTimer();
  }

}
