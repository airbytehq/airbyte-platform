/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.stream;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Common code for operations on a {@link Stream}.
 */
public class MoreStreams {

  /**
   * Operate on an iterator as a stream.
   *
   * @param iterator to access as a stream
   * @param <T> type of the values in the iterator
   * @return stream access to the iterator
   */
  public static <T> Stream<T> toStream(final Iterator<T> iterator) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
  }

  /**
   * Operate on an iteratable as a stream.
   *
   * @param iterable to access as a stream
   * @param <T> type of the values in the iterator
   * @return stream access to the iterator
   */
  public static <T> Stream<T> toStream(final Iterable<T> iterable) {
    return toStream(iterable.iterator());
  }

}
