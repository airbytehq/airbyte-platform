/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.perf;

import static io.airbyte.workers.perf.PerfFactory.sleep;

import io.airbyte.config.WorkerSourceConfig;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.workers.internal.AirbyteSource;
import io.airbyte.workers.perf.PerfFactory.SourceConfig;
import io.airbyte.workers.test_utils.AirbyteMessageUtils;
import java.nio.file.Path;
import java.util.Optional;

class PerfSourceFactory {

  record PerfAirbyteSource(SourceHelper helper) implements AirbyteSource {

    @Override
    public void start(WorkerSourceConfig sourceConfig, Path jobRoot) throws Exception {}

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
    public void close() throws Exception {}

    @Override
    public void cancel() throws Exception {}

  }

  static class SourceHelper {

    private final SourceConfig config;
    private int readNumRecords;
    private int readNumLogs;
    private int readNumStates;
    private boolean initReadWait;
    private boolean finished;
    // ensure we don't get stuck in
    private boolean lastMessageLog;
    private boolean lastMessageState;

    SourceHelper(final SourceConfig config) {
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

      // should log if the previous message wasn't a log, log-every is defined, and we're at the log-every
      // number of messages
      final var shouldLog = !lastMessageLog && config.readLogEvery() > 0 && readNumRecords % config.readLogEvery() == 0;
      // should state if we're on the last record (the last message should always be a state) OR
      // the previous message wasn't a state, state-every is defined, and we're on the state-every number
      // of messages
      final var shouldState = readNumRecords == config.readNumRecords() || (!lastMessageState && (config.readStateEvery() > 0
          && readNumRecords % config.readStateEvery() == 0));

      if (shouldLog) {
        message = config.readLogs().get((readNumLogs % config.readLogs().size()) - 1);
        readNumLogs++;
        lastMessageLog = true;
        lastMessageState = false;
      } else if (shouldState) {
        final var stateStream = AirbyteMessageUtils.createStreamStateMessage("perf/stream-0", readNumRecords);
        message = new AirbyteMessage().withType(Type.STATE).withState(stateStream);
        readNumStates++;
        lastMessageLog = false;
        lastMessageState = true;
      } else {
        // typical message
        message = config.readRecords().get((readNumRecords % config.readRecords().size()) - 1);
        readNumRecords++;
        if (config.readBatchSize() > 0 && readNumRecords % config.readBatchSize() == 0) {
          sleep(config.readBatchWait());
        }
        lastMessageLog = false;
        lastMessageState = false;
      }

      if (readNumRecords == config.readNumRecords()) {
        finished = true;
      }

      return message;
    }

    boolean isFinished() {
      return finished;
    }

    int exitValue() {
      return config.exitValue();
    }

  }

}
