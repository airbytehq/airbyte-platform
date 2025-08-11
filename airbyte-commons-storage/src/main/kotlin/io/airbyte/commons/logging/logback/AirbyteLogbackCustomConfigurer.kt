/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.sift.SiftingAppender
import ch.qos.logback.classic.spi.Configurator
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.Context
import ch.qos.logback.core.Layout
import ch.qos.logback.core.boolex.EventEvaluator
import ch.qos.logback.core.boolex.EventEvaluatorBase
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.encoder.LayoutWrappingEncoder
import ch.qos.logback.core.filter.EvaluatorFilter
import ch.qos.logback.core.hook.DefaultShutdownHook
import ch.qos.logback.core.sift.AppenderFactory
import ch.qos.logback.core.sift.Discriminator
import ch.qos.logback.core.spi.ContextAwareBase
import ch.qos.logback.core.spi.FilterReply
import ch.qos.logback.core.util.Duration
import ch.qos.logback.core.util.StatusPrinter2
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.logging.DEFAULT_AUDIT_LOGGING_PATH_MDC_KEY
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.LOG_SOURCE_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.storage.CloudStorageBulkUploaderExecutor
import io.airbyte.commons.storage.DocumentType
import org.slf4j.Logger.ROOT_LOGGER_NAME

private const val DEFAULT_APPENDER_TIMEOUT_MIN = "15"
val APPENDER_TIMEOUT = EnvVar.LOG_IDLE_ROUTE_TTL.fetchNotNull(default = DEFAULT_APPENDER_TIMEOUT_MIN)

/**
 * Custom Logback [Configurator] that configures Logback appenders and loggers for use in the platform.  This configurator allows us to
 * dynamically control the output of each logger and apply any additional logic prior to logging the message.
 */
class AirbyteLogbackCustomConfigurer :
  ContextAwareBase(),
  Configurator {
  override fun configure(loggerContext: LoggerContext): Configurator.ExecutionStatus {
    // Ensure that the logging context is stopped on application shutdown
    registerShutdownHook(loggerContext = loggerContext)

    // Output any configuration errors
    StatusPrinter2().printInCaseOfErrorsOrWarnings(loggerContext)

    // Create appenders
    val appenders =
      listOf(
        createPlatformAppender(loggerContext = loggerContext),
        createOperationsJobAppender(loggerContext = loggerContext),
        createReplicationDumpAppender(loggerContext = loggerContext),
        createAuditLogAppender(loggerContext = loggerContext),
      )

    // Register appenders with root logger
    loggerContext.getLogger(ROOT_LOGGER_NAME).apply {
      level = getLogLevel()
      isAdditive = true
      appenders.forEach { addAppender(it) }
    }

    // Disable noise from Jooq. https://github.com/jOOQ/jOOQ/issues/4019
    loggerContext.getLogger("org.jooq.Constants").level = Level.OFF

    // Do not allow any other configurators to run after this.
    // This prevents Logback from creating the default console appender for the root logger.
    return Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY
  }

  /**
   * Custom [EventEvaluator] that filters logging events based on log level thresholds.
   * Events below the specified threshold level will be denied (filtered out).
   */
  class ThresholdEvaluator : EventEvaluatorBase<ILoggingEvent>() {
    /**
     * The minimum log level threshold. Events below this level will be filtered out.
     */
    var threshold: Level? = null

    /**
     * Evaluates whether a logging event should be denied based on its level.
     *
     * @param event The logging event to evaluate.
     * @return `true` to deny the event (filter out), `false` to allow the event through.
     */
    override fun evaluate(event: ILoggingEvent): Boolean {
      val thresholdLevel = threshold ?: return false
      return event.level.levelInt < thresholdLevel.levelInt
    }
  }

  /**
   * Custom [EventEvaluator] that filters logging events based on ReplicationDebugLogLevelEnabled feature flag.
   * Events are only allowed through when running in orchestrator mode AND debug level is enabled.
   */
  class ReplicationDebugEvaluator : EventEvaluatorBase<ILoggingEvent>() {
    /**
     * Evaluates whether a logging event should be denied.
     *
     * @param event The logging event to evaluate.
     * @return `true` to deny the event (filter out), `false` to allow the event through.
     */
    override fun evaluate(event: ILoggingEvent): Boolean {
      // Check if we're in a replication-related component (orchestrator, source, or destination)
      val eventLogSource = event.mdcPropertyMap?.get(LOG_SOURCE_MDC_KEY)
      val isReplicationComponent =
        eventLogSource in
          setOf(
            LogSource.REPLICATION_ORCHESTRATOR.displayName,
            LogSource.SOURCE.displayName,
            LogSource.DESTINATION.displayName,
          )

      if (!isReplicationComponent) {
        return true // Deny if not a replication component
      }

      // Check if the current logger context has DEBUG level enabled
      val loggerContext = context as LoggerContext
      val rootLogger = loggerContext.getLogger(ROOT_LOGGER_NAME)
      val isDebugLevel = rootLogger.level == Level.DEBUG

      return !isDebugLevel // Return true to DENY if not debug, false to ALLOW if debug
    }
  }

  /**
   * Builds the appender for operations job log messages.  This appender logs all messages to remote storage.
   * Only logs events at INFO level and above.
   *
   * @param loggerContext The logging context.
   * @return The operations job appender.
   */
  private fun createOperationsJobAppender(loggerContext: LoggerContext): Appender<ILoggingEvent> {
    val appenderFactory = { context: Context, discriminatorValue: String ->
      createCloudAppender(
        context = context,
        discriminatorValue = discriminatorValue,
        documentType = DocumentType.LOGS,
        appenderName = CLOUD_OPERATIONS_JOB_LOGGER_NAME,
      )
    }

    val thresholdEvaluator =
      ThresholdEvaluator().apply {
        context = loggerContext
        threshold = Level.INFO
        start()
      }

    return createSiftingAppender(
      appenderFactory = appenderFactory,
      appenderName = CLOUD_OPERATIONS_JOB_LOGGER_NAME,
      // Uses the same MDC KEY as job loggings
      contextKey = DEFAULT_JOB_LOG_PATH_MDC_KEY,
      loggerContext = loggerContext,
      evaluators = listOf(thresholdEvaluator),
    )
  }

  /**
   * Builds the appender for replication job dumped messages. This appender logs all messages to remote storage,
   * but only for events from the replication orchestrator log source. This specialized appender is used for
   * debugging purposes to capture specific log events from replication jobs.
   * Only logs events at DEBUG level and above.
   *
   * @param loggerContext The logging context.
   * @return The replication job dump appender with log source filtering.
   */
  internal fun createReplicationDumpAppender(loggerContext: LoggerContext): Appender<ILoggingEvent> {
    val appenderFactory = { context: Context, discriminatorValue: String ->
      createCloudAppender(
        context = context,
        discriminatorValue = discriminatorValue,
        documentType = DocumentType.REPLICATION_DUMP,
        appenderName = CLOUD_REPLICATION_JOB_DUMPER_NAME,
      )
    }

    val thresholdEvaluator =
      ThresholdEvaluator().apply {
        context = loggerContext
        threshold = Level.DEBUG
        start()
      }

    val replicationDebugEvaluator =
      ReplicationDebugEvaluator().apply {
        context = loggerContext
        start()
      }

    return createSiftingAppender(
      appenderFactory = appenderFactory,
      appenderName = CLOUD_REPLICATION_JOB_DUMPER_NAME,
      contextKey = DEFAULT_JOB_LOG_PATH_MDC_KEY,
      loggerContext = loggerContext,
      evaluators = listOf(thresholdEvaluator, replicationDebugEvaluator),
    )
  }

  /**
   * Builds the appender for audit log messages.  This appender logs all messages to remote storage.
   *
   * @param loggerContext The logging context.
   * @return The operations audit log appender.
   */
  private fun createAuditLogAppender(loggerContext: LoggerContext): Appender<ILoggingEvent> {
    val appenderFactory = { context: Context, discriminatorValue: String ->
      createCloudAppender(
        context = context,
        discriminatorValue = discriminatorValue,
        documentType = DocumentType.AUDIT_LOGS,
        appenderName = AUDIT_LOGGER_NAME,
      )
    }

    return createSiftingAppender(
      appenderFactory = appenderFactory,
      appenderName = AUDIT_LOGGER_NAME,
      contextKey = DEFAULT_AUDIT_LOGGING_PATH_MDC_KEY,
      loggerContext = loggerContext,
    )
  }

  /**
   * Builds an [AirbyteCloudStorageAppender] for remote logging of log messages.
   *
   * @param context The logging context.
   * @param discriminatorValue The discriminator value used to select this appender.
   * @param documentType The remote storage [DocumentType].
   * @param appenderName The base appender name.
   * @return An [AirbyteCloudStorageAppender] used to store logs remotely.
   */
  internal fun createCloudAppender(
    context: Context,
    discriminatorValue: String,
    documentType: DocumentType,
    appenderName: String,
  ): AirbyteCloudStorageAppender {
    val appender =
      AirbyteCloudStorageAppender(
        baseStorageId = discriminatorValue,
        documentType = documentType,
      )
    appender.context = context
    appender.name = "$appenderName-$discriminatorValue"
    appender.start()
    return appender
  }

  /**
   * Builds the appender for platform log messages.  This appender logs all messages to the console.
   *
   * @param loggerContext The logging context.
   * @return The platform appender.
   */
  internal fun createPlatformAppender(loggerContext: LoggerContext): ConsoleAppender<ILoggingEvent> =
    ConsoleAppender<ILoggingEvent>().apply {
      context = loggerContext
      encoder =
        if (EnvVar.PLATFORM_LOG_FORMAT.fetchNotNull().lowercase() == "json") {
          AirbyteLogEventEncoder().apply { start() }
        } else {
          createUnstructuredEncoder(context = loggerContext, layout = AirbytePlatformLogbackMessageLayout())
        }
      name = PLATFORM_LOGGER_NAME
      start()
    }

  /**
   * Builds a [Discriminator] that is used to extract a value from the logging MDC.
   *
   * @param contextKey The key in the MDC that will be extracted if present and not blank.
   * @param loggerContext The logging context.
   * @return The [Discriminator].
   */
  private fun createDiscriminator(
    contextKey: String,
    loggerContext: LoggerContext,
  ): Discriminator<ILoggingEvent> =
    AirbyteStorageMDCBasedDiscriminator(mdcValueExtractor = { mdc -> mdc[contextKey] ?: "" }).apply {
      context = loggerContext
      start()
    }

  /**
   * Builds the [Encoder] used to generated unstructured log events.
   *
   * @param context The logging [Context].
   * @param layout The logging message [Layout] to be applied to the message.
   * @return The [Encoder].
   */
  private fun createUnstructuredEncoder(
    context: Context,
    layout: Layout<ILoggingEvent>,
  ): Encoder<ILoggingEvent> {
    layout.context = context
    layout.start()

    return LayoutWrappingEncoder<ILoggingEvent>().apply {
      this.context = context
      this.layout = layout
    }
  }

  /**
   * Builds an [EventEvaluator] that tests whether the MDC contains a non-blank value
   * for the provided `contextKey`.  This evaluator is used to avoid routing logging
   * events to the [SiftingAppender] when the event does not contain the required MDC property.
   *
   * @param contextKey The key in the MDC to be checked.
   * @param loggerContext The logging context.
   * @return The [EventEvaluator] that checks the provided `contextKey` in the MDC.
   */
  private fun createEvaluator(
    contextKey: String,
    loggerContext: LoggerContext,
  ): EventEvaluator<ILoggingEvent> =
    AirbyteMdcEvaluator(contextKey = contextKey).apply {
      context = loggerContext
      start()
    }

  /**
   * Builds an [EvaluatorFilter] that denys matching the logging event
   * to the [SiftingAppender] if the provided [EventEvaluator] expression
   * returns `true`.  This is used to avoid routing events with missing MDC properties
   * to the appender.
   *
   * @param evaluator An [EventEvaluator] to be used by the filter.
   * @param loggerContext The logging context.
   * @return An [EvaluatorFilter] that denies matches when the provided evaluator results in a `true` comparison.
   */
  private fun createFilter(
    evaluator: EventEvaluator<ILoggingEvent>,
    loggerContext: LoggerContext,
  ): EvaluatorFilter<ILoggingEvent> =
    EvaluatorFilter<ILoggingEvent>().apply {
      context = loggerContext
      this.evaluator = evaluator
      onMatch = FilterReply.DENY
      onMismatch = FilterReply.NEUTRAL
      start()
    }

  /**
   * Builds a [SiftingAppender] that is invoked when the provided `contextKey` is present
   * in the MDC. Once created, the appender will expire after disuse to ensure proper cleanup.
   * Optionally accepts custom evaluators for additional filtering logic beyond MDC key presence.
   *
   * @param appenderFactory An [AppenderFactory] used to create an appender when the logging event matches the provided filter.
   * @param contextKey The key in the MDC that is used to filter logging events.
   * @param appenderName The name to apply to the appender.
   * @param loggerContext The logging context.
   * @param evaluators Optional list of custom [EventEvaluator]s for additional filtering logic. If not provided, defaults to checking MDC key presence.
   * @return A [SiftingAppender] that creates dynamic appenders based on the value returned by a [Discriminator].
   */
  internal fun createSiftingAppender(
    appenderFactory: AppenderFactory<ILoggingEvent>,
    contextKey: String,
    appenderName: String,
    loggerContext: LoggerContext,
    evaluators: List<EventEvaluator<ILoggingEvent>> = emptyList(),
  ): SiftingAppender {
    val discriminator = createDiscriminator(contextKey = contextKey, loggerContext = loggerContext)

    return SiftingAppender().apply {
      setAppenderFactory(appenderFactory)
      context = loggerContext
      this.discriminator = discriminator
      name = appenderName
      timeout = Duration.valueOf("$APPENDER_TIMEOUT minutes")

      // Add MDC key presence filter by default
      if (evaluators.isEmpty()) {
        addFilter(createFilter(evaluator = createEvaluator(contextKey = contextKey, loggerContext = loggerContext), loggerContext = loggerContext))
      } else {
        // Add MDC key presence filter first
        addFilter(createFilter(evaluator = createEvaluator(contextKey = contextKey, loggerContext = loggerContext), loggerContext = loggerContext))
        // Add all custom evaluator filters
        evaluators.forEach { evaluator ->
          addFilter(createFilter(evaluator = evaluator, loggerContext = loggerContext))
        }
      }

      start()
    }
  }

  /**
   * Registers a shutdown hook with the JVM to ensure that the logging context is stopped
   * on JVM exit.  This ensures that any active appender is stopped, allowing them to
   * publish any pending logging events.
   *
   * @param loggerContext The logging context.
   */
  private fun registerShutdownHook(loggerContext: LoggerContext) {
    Runtime.getRuntime().addShutdownHook(
      Thread { AirbyteLogbackShutdownHook().apply { context = loggerContext }.run() },
    )
  }
}

private fun getLogLevel(): Level = Level.toLevel(EnvVar.LOG_LEVEL.fetchNotNull(default = Level.INFO.toString()))

/**
 * Custom Logback [ch.qos.logback.core.hook.ShutdownHook] implementation that ensures that all Logback appenders
 * are shut down when the JVM exits AND that the executor service used to upload log events to cloud storage is flushed on exit.
 */
class AirbyteLogbackShutdownHook : DefaultShutdownHook() {
  override fun run() {
    super.stop()
    CloudStorageBulkUploaderExecutor.stopAirbyteCloudStorageAppenderExecutorService()
  }
}
