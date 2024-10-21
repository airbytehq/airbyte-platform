/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.boolex.JaninoEventEvaluator
import ch.qos.logback.classic.sift.SiftingAppender
import ch.qos.logback.classic.spi.Configurator
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.Context
import ch.qos.logback.core.Layout
import ch.qos.logback.core.boolex.EventEvaluator
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
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.storage.DocumentType
import org.slf4j.Logger.ROOT_LOGGER_NAME

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
      )

    // Register appenders with root logger
    loggerContext.getLogger(ROOT_LOGGER_NAME).apply {
      level = getLogLevel()
      isAdditive = true
      appenders.forEach { addAppender(it) }
    }

    // Do not allow any other configurators to run after this.
    // This prevents Logback from creating the default console appender for the root logger.
    return Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY
  }

  /**
   * Builds the appender for operations job log messages.  This appender logs all messages to remote storage.
   *
   * @param loggerContext The logging context.
   * @return The operations job appender.
   */
  private fun createOperationsJobAppender(loggerContext: LoggerContext): Appender<ILoggingEvent> {
    val appenderFactory = { context: Context, discriminatorValue: String ->
      createCloudAppender(
        context = context,
        discriminatorValue = discriminatorValue,
        layout = AirbyteOperationsJobLogbackMessageLayout(),
        documentType = DocumentType.LOGS,
        appenderName = CLOUD_OPERATIONS_JOB_LOGGER_NAME,
      )
    }

    return createSiftingAppender(
      appenderFactory = appenderFactory,
      appenderName = CLOUD_OPERATIONS_JOB_LOGGER_NAME,
      contextKey = DEFAULT_JOB_LOG_PATH_MDC_KEY,
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
   * @param layout The log message [Layout].
   * @return An [AirbyteCloudStorageAppender] used to store logs remotely.
   */
  internal fun createCloudAppender(
    context: Context,
    discriminatorValue: String,
    documentType: DocumentType,
    appenderName: String,
    layout: Layout<ILoggingEvent>,
  ): AirbyteCloudStorageAppender {
    val appender =
      AirbyteCloudStorageAppender(
        encoder = createEncoder(context = context, layout = layout),
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
      encoder = createEncoder(context = loggerContext, layout = AirbytePlatformLogbackMessageLayout())
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
   * Builds the [Encoder] used to format the logging event message.
   *
   * @param context The logging [Context].
   * @param layout The logging message [Layout] to be applied to the message.
   * @return The [Encoder].
   */
  private fun createEncoder(
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
    JaninoEventEvaluator().apply {
      context = loggerContext
      expression = """mdc.get("$contextKey") == null || mdc.get("$contextKey") == """""
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
   * in the MDC.  Once created, the appender will expire after disuse to ensure proper cleanup.
   *
   * @param appenderFactory An [AppenderFactory] used to create an appender when the logging event matches the provided filter.
   * @param contextKey The key in the MDC that is used to filter logging events.
   * @param appenderName The name to apply to the appender.
   * @param loggerContext The logging context.
   * @return A [SiftingAppender] that creates dynamic appenders based on the value returned by a [Discriminator].
   */
  internal fun createSiftingAppender(
    appenderFactory: AppenderFactory<ILoggingEvent>,
    contextKey: String,
    appenderName: String,
    loggerContext: LoggerContext,
  ): SiftingAppender {
    val discriminator = createDiscriminator(contextKey = contextKey, loggerContext = loggerContext)
    val evaluator = createEvaluator(contextKey = contextKey, loggerContext = loggerContext)
    val filter = createFilter(evaluator = evaluator, loggerContext = loggerContext)

    return SiftingAppender().apply {
      setAppenderFactory(appenderFactory)
      context = loggerContext
      this.discriminator = discriminator
      name = appenderName
      timeout = Duration.valueOf("$APPENDER_TIMEOUT minutes")
      addFilter(filter)
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
    val shutdownHook = DefaultShutdownHook().apply { context = loggerContext }
    Runtime.getRuntime().addShutdownHook(
      Thread {
        stopAirbyteCloudStorageAppenderExecutorService()
        shutdownHook.run()
      },
    )
  }
}

private const val DEFAULT_APPENDER_TIMEOUT_MIN = "15"
val APPENDER_TIMEOUT = EnvVar.LOG_IDLE_ROUTE_TTL.fetchNotNull(default = DEFAULT_APPENDER_TIMEOUT_MIN)

private fun getLogLevel(): Level = Level.toLevel(EnvVar.LOG_LEVEL.fetchNotNull(default = Level.INFO.toString()))
