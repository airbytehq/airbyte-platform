/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang;

import java.util.Queue;

/**
 * {@link Queue} that implements {@link AutoCloseable}.
 *
 * @param <E> type of value in the queue
 */
public interface CloseableQueue<E> extends Queue<E>, AutoCloseable {

}
