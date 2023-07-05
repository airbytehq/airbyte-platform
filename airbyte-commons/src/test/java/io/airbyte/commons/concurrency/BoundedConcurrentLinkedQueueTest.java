/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BoundedConcurrentLinkedQueueTest {

  private static final int defaultMaxSize = 3;

  private record Record(int value) {}

  private final Record record1 = new Record(1);
  private final Record record2 = new Record(2);
  private final Record record3 = new Record(3);

  private BoundedConcurrentLinkedQueue<Record> getQueue(int maxSize) {
    return new BoundedConcurrentLinkedQueue<>(maxSize);
  }

  @Test
  void testBasicAddPollBehavior() {
    final BoundedConcurrentLinkedQueue<Record> queue = getQueue(defaultMaxSize);

    final List<Record> records = List.of(
        new Record(1),
        new Record(2),
        new Record(3),
        new Record(4));

    final List<Boolean> insertionResults = records.stream().map(queue::add).toList();

    // The last item is false because defaultMax size is 3 so the last insert should fail
    assertEquals(List.of(true, true, true, false), insertionResults);

    queue.close();

    final List<Record> readRecords = new ArrayList<>();
    while (!queue.isDone()) {
      readRecords.add(queue.poll());
    }
    assertEquals(records.subList(0, 3), readRecords);
  }

  @Test
  void testBasicAddPoll() {
    final BoundedConcurrentLinkedQueue<Record> queue = getQueue(2);

    assertEquals(0, queue.size());
    queue.add(record1);
    assertEquals(1, queue.size());
    queue.add(record2);
    assertEquals(2, queue.size());

    assertEquals(record1, queue.poll());
    assertEquals(1, queue.size());
    assertEquals(record2, queue.poll());
    assertEquals(0, queue.size());

    assertNull(queue.poll());
    // Extra poll shouldn't decrement the size
    assertEquals(0, queue.size());
  }

  @Test
  void testAddReturnsFalseIfQueueIsFull() {
    final BoundedConcurrentLinkedQueue<Record> queue = getQueue(1);

    assertTrue(queue.add(new Record(42)));
    assertFalse(queue.add(new Record(43)));

    // Extra add shouldn't increment the size
    assertEquals(1, queue.size());
  }

  @Test
  void testAQueueIsDoneIfItIsEmptyAndClosed() {
    final BoundedConcurrentLinkedQueue<Record> queue = getQueue(2);

    queue.add(record3);
    assertFalse(queue.isDone());
    queue.add(record1);
    assertFalse(queue.isDone());

    queue.poll();
    queue.poll();
    assertFalse(queue.isDone());

    queue.add(record2);
    assertFalse(queue.isDone());

    assertFalse(queue.isClosed());
    queue.close();
    assertTrue(queue.isClosed());
    assertFalse(queue.isDone());

    queue.poll();
    assertTrue(queue.isDone());
  }

  @Test
  void testAddToClosedQueueFails() {
    final BoundedConcurrentLinkedQueue<Record> queue = getQueue(defaultMaxSize);

    assertTrue(queue.add(record1));
    queue.close();
    assertFalse(queue.add(record2));
    assertEquals(1, queue.size());
  }

  @Test
  void testAddingNullDoesntIncrementSize() {
    final BoundedConcurrentLinkedQueue<Record> queue = getQueue(defaultMaxSize);

    queue.add(record3);
    assertThrows(NullPointerException.class, () -> queue.add(null));
    queue.add(record2);
    assertEquals(2, queue.size());
  }

}
