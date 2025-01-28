/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.commons.protocol.AirbyteMessageMigrator;
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider;
import io.airbyte.commons.protocol.AirbyteMessageVersionedMigrator;
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory;
import io.airbyte.commons.protocol.ConfiguredAirbyteCatalogMigrator;
import io.airbyte.commons.protocol.serde.AirbyteMessageDeserializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Deserializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Serializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV1Deserializer;
import io.airbyte.commons.protocol.serde.AirbyteMessageV1Serializer;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.protocol.models.AirbyteLogMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.workers.helper.GsonPksExtractor;
import io.micronaut.core.util.StringUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a stream from an input stream. The produced stream attempts to parse each line of the
 * InputStream into a AirbyteMessage. If the line cannot be parsed into a AirbyteMessage it is
 * dropped. Each record MUST be new line separated.
 *
 * If a line starts with a AirbyteMessage and then has other characters after it, that
 * AirbyteMessage will still be parsed. If there are multiple AirbyteMessage records on the same
 * line, only the first will be parsed.
 *
 * Handles parsing and validation from a specific version of the Airbyte Protocol as well as
 * upgrading messages to the current version.
 */
@SuppressWarnings("PMD.MoreThanOneLogger")
public class VersionedAirbyteStreamFactory<T> implements AirbyteStreamFactory {

  public static final String CONNECTION_ID_NOT_PRESENT = "not present";
  public static final String MALFORMED_NON_AIRBYTE_RECORD_LOG_MESSAGE = "Malformed non-Airbyte record (connectionId = {}): {}";
  public static final String MALFORMED_AIRBYTE_RECORD_LOG_MESSAGE = "Malformed Airbyte record (connectionId = {}): {}";

  public record InvalidLineFailureConfiguration(boolean printLongRecordPks) {}

  private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(VersionedAirbyteStreamFactory.class);

  @VisibleForTesting
  static final MdcScope.Builder DEFAULT_MDC_SCOPE = MdcScope.DEFAULT_BUILDER;

  private static final Version fallbackVersion = new Version("0.2.0");

  // Buffer size to use when detecting the protocol version.
  // Given that BufferedReader::reset fails if we try to reset if we go past its buffer size, this
  // buffer has to be big enough to contain our longest spec and whatever messages get emitted before
  // the SPEC.
  private static final int BUFFER_READ_AHEAD_LIMIT = 2 * 1024 * 1024; // 2 megabytes
  private static final int MESSAGES_LOOK_AHEAD_FOR_DETECTION = 10;
  private static final String TYPE_FIELD_NAME = "type";
  private static final int MAXIMUM_CHARACTERS_ALLOWED = 20_000_000;

  // BASIC PROCESSING FIELDS
  protected final Logger logger;
  private final Optional<UUID> connectionId;

  private final MdcScope.Builder containerLogMdcBuilder;

  // VERSION RELATED FIELDS
  private final AirbyteMessageSerDeProvider serDeProvider;
  private final AirbyteProtocolVersionedMigratorFactory migratorFactory;
  private final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog;
  private AirbyteMessageDeserializer<AirbyteMessage> deserializer;
  private AirbyteMessageVersionedMigrator<AirbyteMessage> migrator;
  private Version protocolVersion;

  private boolean shouldDetectVersion = false;

  private final InvalidLineFailureConfiguration invalidLineFailureConfiguration;
  private final GsonPksExtractor gsonPksExtractor;

  /**
   * In some cases, we know the stream will never emit messages that need to be migrated. This is
   * particularly true for tests. This is a convenience method for those cases.
   *
   * @return a VersionedAirbyteStreamFactory that does not perform any migration.
   */
  @VisibleForTesting
  public static VersionedAirbyteStreamFactory noMigrationVersionedAirbyteStreamFactory() {
    return noMigrationVersionedAirbyteStreamFactory(DEFAULT_LOGGER, MdcScope.DEFAULT_BUILDER,
        new InvalidLineFailureConfiguration(false), new GsonPksExtractor());
  }

  /**
   * Same as above with additional config for testing.
   *
   * @return a VersionedAirbyteStreamFactory that does not perform any migration.
   */
  @VisibleForTesting
  public static VersionedAirbyteStreamFactory noMigrationVersionedAirbyteStreamFactory(final Logger logger,
                                                                                       final MdcScope.Builder mdcBuilder,
                                                                                       final InvalidLineFailureConfiguration conf,
                                                                                       final GsonPksExtractor gsonPksExtractor) {
    final AirbyteMessageSerDeProvider provider = new AirbyteMessageSerDeProvider(
        List.of(new AirbyteMessageV0Deserializer(), new AirbyteMessageV1Deserializer()),
        List.of(new AirbyteMessageV0Serializer(), new AirbyteMessageV1Serializer()));
    provider.initialize();

    final AirbyteMessageMigrator airbyteMessageMigrator = new AirbyteMessageMigrator(List.of());
    airbyteMessageMigrator.initialize();
    final ConfiguredAirbyteCatalogMigrator configuredAirbyteCatalogMigrator = new ConfiguredAirbyteCatalogMigrator(List.of());
    configuredAirbyteCatalogMigrator.initialize();
    final AirbyteProtocolVersionedMigratorFactory fac =
        new AirbyteProtocolVersionedMigratorFactory(airbyteMessageMigrator, configuredAirbyteCatalogMigrator);

    return new VersionedAirbyteStreamFactory<>(provider, fac, AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION, Optional.empty(),
        Optional.empty(), logger, mdcBuilder, conf, gsonPksExtractor);
  }

  public VersionedAirbyteStreamFactory(final AirbyteMessageSerDeProvider serDeProvider,
                                       final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                       final Version protocolVersion,
                                       final Optional<UUID> connectionId,
                                       final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog,
                                       final MdcScope.Builder containerLogMdcBuilder,
                                       final InvalidLineFailureConfiguration invalidLineFailureConfiguration,
                                       final GsonPksExtractor gsonPksExtractor) {
    this(serDeProvider, migratorFactory, protocolVersion, connectionId, configuredAirbyteCatalog, DEFAULT_LOGGER, containerLogMdcBuilder,
        invalidLineFailureConfiguration, gsonPksExtractor);
  }

  public VersionedAirbyteStreamFactory(final AirbyteMessageSerDeProvider serDeProvider,
                                       final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                       final Version protocolVersion,
                                       final Optional<UUID> connectionId,
                                       final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog,
                                       final InvalidLineFailureConfiguration invalidLineFailureConfiguration,
                                       final GsonPksExtractor gsonPksExtractor) {
    this(serDeProvider, migratorFactory, protocolVersion, connectionId, configuredAirbyteCatalog, DEFAULT_LOGGER, DEFAULT_MDC_SCOPE,
        invalidLineFailureConfiguration, gsonPksExtractor);
  }

  public VersionedAirbyteStreamFactory(final AirbyteMessageSerDeProvider serDeProvider,
                                       final AirbyteProtocolVersionedMigratorFactory migratorFactory,
                                       final Version protocolVersion,
                                       final Optional<UUID> connectionId,
                                       final Optional<ConfiguredAirbyteCatalog> configuredAirbyteCatalog,
                                       final Logger logger,
                                       final MdcScope.Builder containerLogMdcBuilder,
                                       final InvalidLineFailureConfiguration invalidLineFailureConfiguration,
                                       final GsonPksExtractor gsonPksExtractor) {
    // TODO AirbyteProtocolPredicate needs to be updated to be protocol version aware
    this.logger = logger;
    this.containerLogMdcBuilder = containerLogMdcBuilder;
    this.gsonPksExtractor = gsonPksExtractor;

    Preconditions.checkNotNull(protocolVersion);
    this.serDeProvider = serDeProvider;
    this.migratorFactory = migratorFactory;
    this.configuredAirbyteCatalog = configuredAirbyteCatalog;
    this.initializeForProtocolVersion(protocolVersion);
    this.connectionId = connectionId;
    this.invalidLineFailureConfiguration = invalidLineFailureConfiguration;
  }

  /**
   * Create the AirbyteMessage stream.
   *
   * If detectVersion is set to true, it will decide which protocol version to use from the content of
   * the stream rather than the one passed from the constructor.
   */
  @Override
  public Stream<AirbyteMessage> create(final BufferedReader bufferedReader) {
    detectAndInitialiseMigrators(bufferedReader);
    final boolean needMigration = !protocolVersion.getMajorVersion().equals(migratorFactory.getMostRecentVersion().getMajorVersion());
    logger.info(
        "Reading messages from protocol version {}{}",
        protocolVersion.serialize(),
        needMigration ? ", messages will be upgraded to protocol version " + migratorFactory.getMostRecentVersion().serialize() : "");

    return addLineReadLogic(bufferedReader);
  }

  private void detectAndInitialiseMigrators(final BufferedReader bufferedReader) {
    if (shouldDetectVersion) {
      final Optional<Version> versionMaybe;
      try {
        versionMaybe = detectVersion(bufferedReader);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      if (versionMaybe.isPresent()) {
        logger.info("Detected Protocol Version {}", versionMaybe.get().serialize());
        initializeForProtocolVersion(versionMaybe.get());
      } else {
        // No version found, use the default as a fallback
        logger.info("Unable to detect Protocol Version, assuming protocol version {}", fallbackVersion.serialize());
        initializeForProtocolVersion(fallbackVersion);
      }
    }
  }

  private Stream<AirbyteMessage> addLineReadLogic(final BufferedReader bufferedReader) {
    final var metricClient = MetricClientFactory.getMetricClient();
    return bufferedReader
        .lines()
        .peek(str -> {
          final long messageSize = str.getBytes(StandardCharsets.UTF_8).length;
          metricClient.distribution(OssMetricsRegistry.JSON_STRING_LENGTH, messageSize);
        })
        .flatMap(this::toAirbyteMessage)
        .filter(this::filterLog);
  }

  /**
   * Attempt to detect the version by scanning the stream
   *
   * Using the BufferedReader reset/mark feature to get a look-ahead. We will attempt to find the
   * first SPEC message and decide on a protocol version from this message.
   *
   * @param bufferedReader the stream to read
   * @return The Version if found
   * @throws IOException exception while writing
   */
  private Optional<Version> detectVersion(final BufferedReader bufferedReader) throws IOException {
    // Buffersize needs to be big enough to containing everything we need for the detection. Otherwise,
    // the reset will fail.
    bufferedReader.mark(BUFFER_READ_AHEAD_LIMIT);
    try {
      // Cap detection to the first 10 messages. When doing the protocol detection, we expect the SPEC
      // message to show up early in the stream. Ideally it should be first message however we do not
      // enforce this constraint currently so connectors may send LOG messages before.
      for (int i = 0; i < MESSAGES_LOOK_AHEAD_FOR_DETECTION; ++i) {
        final String line = bufferedReader.readLine();
        final Optional<JsonNode> jsonOpt = Jsons.tryDeserialize(line);
        if (jsonOpt.isPresent()) {
          final JsonNode json = jsonOpt.get();
          if (isSpecMessage(json)) {
            final JsonNode protocolVersionNode = json.at("/spec/protocol_version");
            bufferedReader.reset();
            return Optional.ofNullable(protocolVersionNode).filter(Predicate.not(JsonNode::isMissingNode)).map(node -> new Version(node.asText()));
          }
        }
      }
      bufferedReader.reset();
      return Optional.empty();
    } catch (final IOException e) {
      logger.warn(
          "Protocol version detection failed, it is likely than the connector sent more than {}B without an complete SPEC message."
              + " A SPEC message that is too long could be the root cause here.",
          BUFFER_READ_AHEAD_LIMIT);
      throw e;
    }
  }

  private boolean isSpecMessage(final JsonNode json) {
    return json.has(TYPE_FIELD_NAME) && "spec".equalsIgnoreCase(json.get(TYPE_FIELD_NAME).asText());
  }

  public boolean setDetectVersion(final boolean detectVersion) {
    return this.shouldDetectVersion = detectVersion;
  }

  public VersionedAirbyteStreamFactory<T> withDetectVersion(final boolean detectVersion) {
    setDetectVersion(detectVersion);
    return this;
  }

  protected final void initializeForProtocolVersion(final Version protocolVersion) {
    this.deserializer = (AirbyteMessageDeserializer<AirbyteMessage>) serDeProvider.getDeserializer(protocolVersion).orElseThrow();
    this.migrator = migratorFactory.getAirbyteMessageMigrator(protocolVersion);
    this.protocolVersion = protocolVersion;
  }

  protected boolean filterLog(final AirbyteMessage message) {
    final boolean isLog = message.getType() == AirbyteMessage.Type.LOG;
    if (isLog) {
      try (final var ignored = containerLogMdcBuilder.build()) {
        internalLog(message.getLog());
      }
    }
    return !isLog;
  }

  protected void internalLog(final AirbyteLogMessage logMessage) {
    final String combinedMessage =
        logMessage.getMessage() + (logMessage.getStackTrace() != null ? (System.lineSeparator()
            + "Stack Trace: " + logMessage.getStackTrace()) : "");

    switch (logMessage.getLevel()) {
      case FATAL, ERROR -> logger.error(combinedMessage);
      case WARN -> logger.warn(combinedMessage);
      case DEBUG -> logger.debug(combinedMessage);
      case TRACE -> logger.trace(combinedMessage);
      default -> logger.info(combinedMessage);
    }
  }

  /**
   * For every incoming message,
   * <p>
   * 1. deserialize the incoming JSON string to {@link AirbyteMessage}.
   * <p>
   * 2. validate the message.
   * <p>
   * 3. upgrade the message to the platform version, if needed.
   */
  protected Stream<AirbyteMessage> toAirbyteMessage(final String line) {
    logLargeRecordWarning(line);

    Optional<AirbyteMessage> m = deserializer.deserializeExact(line);

    if (m.isPresent()) {
      m = BasicAirbyteMessageValidator.validate(m.get(), configuredAirbyteCatalog);

      if (m.isEmpty()) {
        logger.error("Validation failed: {}", Jsons.serialize(line));
        return m.stream();
      }

      return upgradeMessage(m.get());
    }

    logMalformedLogMessage(line);
    return m.stream();
  }

  private void logLargeRecordWarning(final String line) {
    try (final MdcScope ignored = containerLogMdcBuilder.build()) {
      if (line.length() >= MAXIMUM_CHARACTERS_ALLOWED) {
        connectionId.ifPresentOrElse(c -> MetricClientFactory.getMetricClient().count(OssMetricsRegistry.LINE_SKIPPED_TOO_LONG, 1,
            new MetricAttribute(MetricTags.CONNECTION_ID, c.toString())),
            () -> MetricClientFactory.getMetricClient().count(OssMetricsRegistry.LINE_SKIPPED_TOO_LONG, 1));
        MetricClientFactory.getMetricClient().distribution(OssMetricsRegistry.TOO_LONG_LINES_DISTRIBUTION, line.length());
        if (invalidLineFailureConfiguration.printLongRecordPks) {
          logger.warn("[LARGE RECORD] Risk of Destinations not being able to properly handle: " + line.length());
          configuredAirbyteCatalog.ifPresent(
              airbyteCatalog -> logger
                  .warn("[LARGE RECORD] The primary keys of the long record are: " + gsonPksExtractor.extractPks(airbyteCatalog, line)));
        }
      }
    } catch (final Exception e) {
      throw e;
    }
  }

  /**
   * If a line cannot be deserialized into an AirbyteMessage, either:
   * <p>
   * 1) We ran into serialization errors, e.g. too big, garbled etc. The most common error being too
   * big.
   * <p>
   * 2) It is a log message that should be an Airbyte Log Message. Currently, the protocol allows for
   * connectors to log to standard out. This is not ideal as it makes it difficult to distinguish
   * between proper and garbled messages. However, since all Java connectors (both source and
   * destination) currently do this, we cannot change this behaviour today, though in the long term we
   * want to amend the Protocol and strictly enforce this.
   * <p>
   *
   */
  private void logMalformedLogMessage(final String line) {
    try (final MdcScope ignored = containerLogMdcBuilder.build()) {
      if (line.toLowerCase().replaceAll("\\s", "").contains("{\"type\":\"record\",\"record\":")) {
        // Connectors can sometimes log error messages from failing to parse an AirbyteRecordMessage.
        // Filter on record into debug to try and prevent such cases. Though this catches non-record
        // messages, this is ok as we rather be safe than sorry.
        logger.warn("Could not parse the string received from source, it seems to be a record message");
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.LINE_SKIPPED_WITH_RECORD, 1,
            malformedLogAttributes(line, connectionId));
        logger.debug(MALFORMED_AIRBYTE_RECORD_LOG_MESSAGE, getConnectionId(), line);
      } else {
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NON_AIRBYTE_MESSAGE_LOG_LINE, 1,
            malformedLogAttributes(line, connectionId));
        logger.info(MALFORMED_NON_AIRBYTE_RECORD_LOG_MESSAGE, getConnectionId(), line);
      }
    } catch (final Exception e) {
      throw e;
    }
  }

  protected Stream<AirbyteMessage> upgradeMessage(final AirbyteMessage msg) {
    try {
      final AirbyteMessage message = migrator.upgrade(msg, configuredAirbyteCatalog);
      return Stream.of(message);
    } catch (final RuntimeException e) {
      logger.warn("Failed to upgrade a message from version {}: {}", protocolVersion, Jsons.serialize(msg), e);
      return Stream.empty();
    }
  }

  private MetricAttribute[] malformedLogAttributes(final String line, final Optional<UUID> connectionId) {
    final List<MetricAttribute> attributes = new ArrayList<>();
    attributes.add(new MetricAttribute(MetricTags.MALFORMED_LOG_LINE_LENGTH, String.valueOf(StringUtils.isNotEmpty(line) ? line.length() : 0)));
    connectionId.ifPresent(c -> attributes.add(new MetricAttribute(MetricTags.CONNECTION_ID, c.toString())));
    return attributes.toArray(new MetricAttribute[0]);
  }

  private String getConnectionId() {
    return connectionId.isPresent() ? connectionId.get().toString() : CONNECTION_ID_NOT_PRESENT;
  }

}
