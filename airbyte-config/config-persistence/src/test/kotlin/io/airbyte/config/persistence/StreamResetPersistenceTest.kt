/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.StreamDescriptor
import io.airbyte.db.ContextQueryFunction
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

internal class StreamResetPersistenceTest : BaseConfigDatabaseTest() {
  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    truncateAllTables()

    streamResetPersistence = Mockito.spy<StreamResetPersistence>(StreamResetPersistence(database))
  }

  @Test
  @Throws(Exception::class)
  fun testCreateSameResetTwiceOnlyCreateItOnce() {
    val connectionId = UUID.randomUUID()
    val streamDescriptor1 = StreamDescriptor().withName("n1").withNamespace("ns2")
    val streamDescriptor2 = StreamDescriptor().withName("n2")

    streamResetPersistence!!.createStreamResets(connectionId, listOf(streamDescriptor1, streamDescriptor2))

    val result = streamResetPersistence!!.getStreamResets(connectionId)
    LOGGER.info(database!!.query<String?>(ContextQueryFunction { ctx: DSLContext? -> ctx!!.selectFrom("stream_reset").fetch().toString() }))
    Assertions.assertEquals(2, result.size)

    streamResetPersistence!!.createStreamResets(connectionId, listOf(streamDescriptor1))
    LOGGER.info(database!!.query<String?>(ContextQueryFunction { ctx: DSLContext? -> ctx!!.selectFrom("stream_reset").fetch().toString() }))
    Assertions.assertEquals(2, streamResetPersistence!!.getStreamResets(connectionId).size)

    streamResetPersistence!!.createStreamResets(connectionId, listOf(streamDescriptor2))
    LOGGER.info(database!!.query<String?>(ContextQueryFunction { ctx: DSLContext? -> ctx!!.selectFrom("stream_reset").fetch().toString() }))
    Assertions.assertEquals(2, streamResetPersistence!!.getStreamResets(connectionId).size)
  }

  @Test
  @Throws(Exception::class)
  fun testCreateAndGetAndDeleteStreamResets() {
    val streamResetList: MutableList<StreamDescriptor> = ArrayList<StreamDescriptor>()
    val streamDescriptor1 = StreamDescriptor().withName("stream_name_1").withNamespace("stream_namespace_1")
    val streamDescriptor2 = StreamDescriptor().withName("stream_name_2")
    streamResetList.add(streamDescriptor1)
    streamResetList.add(streamDescriptor2)
    val uuid = UUID.randomUUID()
    streamResetPersistence!!.createStreamResets(uuid, streamResetList.toList())

    val result: List<StreamDescriptor> = streamResetPersistence!!.getStreamResets(uuid)
    Assertions.assertEquals(2, result.size)
    Assertions.assertTrue(
      result
        .stream()
        .anyMatch { streamDescriptor: StreamDescriptor? ->
          "stream_name_1" == streamDescriptor!!.getName() &&
            "stream_namespace_1" == streamDescriptor.getNamespace()
        },
    )
    Assertions.assertTrue(
      result
        .stream()
        .anyMatch { streamDescriptor: StreamDescriptor? ->
          "stream_name_2" == streamDescriptor!!.getName() &&
            streamDescriptor.getNamespace() == null
        },
    )

    streamResetPersistence!!.createStreamResets(
      uuid,
      listOf(StreamDescriptor().withName("stream_name_3").withNamespace("stream_namespace_2")),
    )
    streamResetPersistence!!.deleteStreamResets(uuid, result)

    val resultAfterDeleting = streamResetPersistence!!.getStreamResets(uuid)
    Assertions.assertEquals(1, resultAfterDeleting.size)

    Assertions.assertTrue(
      resultAfterDeleting
        .stream()
        .anyMatch { streamDescriptor: StreamDescriptor? ->
          "stream_name_3" == streamDescriptor!!.getName() &&
            "stream_namespace_2" == streamDescriptor.getNamespace()
        },
    )
  }

  companion object {
    var streamResetPersistence: StreamResetPersistence? = null
    private val LOGGER: Logger = LoggerFactory.getLogger(StreamResetPersistenceTest::class.java)
  }
}
