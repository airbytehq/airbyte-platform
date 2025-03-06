/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io;

import io.airbyte.commons.concurrency.VoidCallable;
import io.airbyte.commons.logging.MdcScope;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Abstraction to consume an {@link InputStream} to completion.
 */
@SuppressWarnings("PMD.UnusedLocalVariable")
public class LineGobbler implements VoidCallable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LineGobbler.class);
  private static final String GENERIC = "generic";

  /**
   * Connect an input stream to be consumed by consumer.
   *
   * @param is input stream
   * @param consumer consumer
   */
  public static void gobble(final InputStream is, final Consumer<String> consumer) {
    gobble(is, consumer, GENERIC, MdcScope.DEFAULT_BUILDER);
  }

  /**
   * Connect a message to be consumed by consumer.
   *
   * @param message message to be consumed
   * @param consumer consumer
   */
  public static void gobble(final String message, final Consumer<String> consumer) {
    final InputStream stringAsSteam = new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8));
    gobble(stringAsSteam, consumer);
  }

  /**
   * Connect an input stream to be consumed by consumer with an {@link MdcScope}.
   *
   * @param is input stream
   * @param consumer consumer
   * @param mdcScopeBuilder mdc scope to be used during consumption
   */
  public static void gobble(final InputStream is, final Consumer<String> consumer, final MdcScope.Builder mdcScopeBuilder) {
    gobble(is, consumer, GENERIC, mdcScopeBuilder);
  }

  /**
   * Connect an input stream to be consumed by consumer with an {@link MdcScope} and caller label.
   *
   * @param is input stream
   * @param consumer consumer
   * @param caller name of caller
   * @param mdcScopeBuilder mdc scope to be used during consumption
   */
  public static void gobble(final InputStream is, final Consumer<String> consumer, final String caller, final MdcScope.Builder mdcScopeBuilder) {
    gobble(is, consumer, caller, mdcScopeBuilder, Executors.newSingleThreadExecutor());
  }

  /**
   * Connect an input stream to be consumed by consumer with an {@link MdcScope}, caller label, and
   * executor.
   *
   * Passing the executor lets you wait to ensure that all lines have been gobbled, since it happens
   * asynchronously.
   *
   * @param is input stream
   * @param consumer consumer
   * @param caller name of caller
   * @param mdcScopeBuilder mdc scope to be used during consumption
   * @param executor executor to run gobbling
   */
  public static void gobble(final InputStream is,
                            final Consumer<String> consumer,
                            final String caller,
                            final MdcScope.Builder mdcScopeBuilder,
                            final ExecutorService executor) {
    if (is != null) {
      final Map<String, String> mdc = MDC.getCopyOfContextMap();
      final var gobbler = new LineGobbler(is, consumer, executor, mdc, caller, mdcScopeBuilder);
      executor.submit(gobbler);
    } else {
      LOGGER.warn("Unable to gobble line(s) from input stream provided by {}:  input stream is null.", caller);
    }
  }

  /**
   * Connect a message to be consumed by LOGGER.info.
   *
   * @param message message to be consumed
   */
  private static void gobble(final String message) {
    gobble(message, LOGGER::info);
  }

  /**
   * Used to emit a visual separator in the user-facing logs indicating a start of a meaningful
   * temporal activity.
   *
   * @param message message to emphasize
   * @deprecated use info logging with correct mdc context instead
   */
  @Deprecated
  public static void startSection(final String message) {
    gobble(formatStartSection(message));
  }

  public static String formatStartSection(final String message) {
    return "\r\n----- START " + message + " -----\r\n\r\n";
  }

  /**
   * Used to emit a visual separator in the user-facing logs indicating a end of a meaningful
   * temporal. activity
   *
   * @param message message to emphasize
   * @deprecated use info logging with correct mdc context instead
   */
  public static void endSection(final String message) {
    gobble(formatEndSection(message));
  }

  public static String formatEndSection(final String message) {
    return "\r\n----- END " + message + " -----\r\n\r\n";
  }

  private final BufferedReader is;
  private final Consumer<String> consumer;
  private final ExecutorService executor;
  private final Map<String, String> mdc;
  private final String caller;
  private final MdcScope.Builder containerLogMdcBuilder;

  LineGobbler(final InputStream is,
              final Consumer<String> consumer,
              final ExecutorService executor,
              final Map<String, String> mdc) {
    this(is, consumer, executor, mdc, GENERIC, MdcScope.DEFAULT_BUILDER);
  }

  LineGobbler(final InputStream is,
              final Consumer<String> consumer,
              final ExecutorService executor,
              final Map<String, String> mdc,
              final String caller,
              final MdcScope.Builder mdcScopeBuilder) {
    this.is = IOs.newBufferedReader(is);
    this.consumer = consumer;
    this.executor = executor;
    this.mdc = mdc;
    this.caller = caller;
    this.containerLogMdcBuilder = mdcScopeBuilder;
  }

  @Override
  public void voidCall() {
    MDC.setContextMap(mdc);
    try {
      String line = is.readLine();
      while (line != null) {
        try (final var mdcScope = containerLogMdcBuilder.build()) {
          consumer.accept(line);
        }
        line = is.readLine();
      }
    } catch (final IOException i) {
      LOGGER.warn("{} gobbler IOException: {}. Typically happens when cancelling a job.", caller, i.getMessage());
    } catch (final Exception e) {
      LOGGER.error("{} gobbler error when reading stream", caller, e);
    } finally {
      executor.shutdown();
    }
  }

}
