/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang;

import java.util.function.Consumer;

/**
 * {@link Consumer} that implements {@link AutoCloseable}. Good for consuming things that use a
 * database connection.
 *
 * @param <T> type of the consumer.
 */
public interface CloseableConsumer<T> extends Consumer<T>, AutoCloseable {}
