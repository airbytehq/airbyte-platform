/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.perf;

import static io.airbyte.workers.perf.PerfFactory.sleep;

import io.airbyte.config.WorkerDestinationConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.internal.AirbyteDestination;
import io.airbyte.workers.perf.PerfFactory.DestinationConfig;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class PerfDestinationFactory {

  record PerfAirbyteDestination(DestinationHelper helper) implements AirbyteDestination {

    @Override
    public void start(WorkerDestinationConfig destinationConfig, Path jobRoot) throws Exception {

    }

    @Override
    public void accept(AirbyteMessage message) throws Exception {
      helper.accept(message);
    }

    @Override
    public void notifyEndOfInput() throws Exception {

    }

    @Override
    public boolean isFinished() {
      return helper.isFinished();
    }

    @Override
    public int getExitValue() {
      return helper.exitValue();
    }

    @Override
    public Optional<AirbyteMessage> attemptRead() {
      return Optional.ofNullable(helper.nextRead());
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void cancel() throws Exception {

    }

  }

  /**
   * Helper class to contain all the complexities supported by the DestinationConfig.
   */
  static class DestinationHelper {

    private final DestinationConfig config;
    private int acceptNumRecords;
    private int acceptNumLogs;
    private int acceptNumStates;

    private boolean initReadWait;
    private boolean initAcceptWait;
    private boolean finished;

    private final BlockingQueue<AirbyteMessage> messages = new LinkedBlockingQueue<>();

    DestinationHelper(final DestinationConfig config) {
      this.config = config;
    }

    AirbyteMessage nextRead() {
      if (finished) {
        return null;
      }

      if (!initReadWait) {
        initReadWait = true;
        sleep(config.readInitialWait());
      }

      final AirbyteMessage message;
      try {
        message = messages.poll(config.readBatchWait().toSeconds(), TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      return message;
    }

    void accept(AirbyteMessage message) {
      if (finished || message == null) {
        return;
      }

      if (!initAcceptWait) {
        initAcceptWait = true;
        sleep(config.acceptInitialWait());
      }

      if (config.acceptBatchSize() > 0 && acceptNumRecords % config.acceptBatchSize() == 0) {
        sleep(config.acceptBatchWait());
      }

      try {
        switch (message.getType()) {
          case STATE -> {
            acceptNumStates++;
            if (config.echoState()) {
              messages.put(message);
            }
          }
          case LOG -> {
            acceptNumLogs++;
            if (config.echoLog()) {
              messages.put(message);
            }
          }
          case RECORD -> acceptNumRecords++;
          default -> {
            // NOOP
          }
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      acceptNumRecords++;

      if (config.acceptBatchSize() > 0 && acceptNumRecords % config.acceptBatchSize() == 0) {
        sleep(config.acceptBatchWait());
      }

      if (acceptNumRecords == config.acceptNumRecords()) {
        finished = true;
      }
    }

    boolean isFinished() {
      return finished;
    }

    int exitValue() {
      return config.exitValue();
    }

  }

}
