/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

class MultiTest {
  @Test
  fun `verify data`() {
    val source = Source("source")
    val workspace = Workspace("workspace")

    Multi(listOf(source, workspace)).also {
      assertEquals("multi", it.kind)
    }
  }

  @Test
  fun `Multi cannot contain another Multi`() {
    val source = Source("source")
    val workspace = Workspace("workspace")
    val multi = Multi(listOf(source, workspace))

    assertThrows<IllegalArgumentException> {
      Multi(listOf(source, workspace, multi))
    }
  }

  @Test
  fun `fetchContexts returns correct results`() {
    val source1 = Source("source1")
    val source2 = Source("source2")
    val workspace = Workspace("workspace")
    val multi = Multi(listOf(source1, source2, workspace))

    assertEquals(listOf(source1, source2), multi.fetchContexts<Source>(), "two source contexts")
    assertEquals(listOf(workspace), multi.fetchContexts<Workspace>(), "one workspace context")
    assertEquals(listOf<Connection>(), multi.fetchContexts<Connection>(), "should be empty as no connection context was provided")
  }

  @Test
  fun `key is an empty string`() {
    val workspace = Workspace("workspace")
    val connection = Connection("connection")
    val source = Source("source")
    val destination = Destination("destination")
    val user = User("user")

    Multi(listOf(user, destination, source, connection, workspace)).also {
      assertTrue(it.key.isEmpty())
      assertTrue(it.attrs.isEmpty())
    }
  }

  @Test
  fun `no contexts is an exception`() {
    assertThrows<IllegalArgumentException> {
      Multi(listOf())
    }
  }

  @Test
  fun `dedupes child contexts`() {
    val result =
      Multi(
        listOf(
          Workspace("workspace"),
          Workspace("workspace"),
          Workspace("workspace"),
          Connection("connection"),
          Connection("connection"),
          Source("source"),
          Destination("destination"),
          Destination("destination"),
        ),
      )

    val expected =
      Multi(
        listOf(
          Workspace("workspace"),
          Connection("connection"),
          Source("source"),
          Destination("destination"),
        ),
      )

    assertEquals(expected, result)
  }

  @Test
  fun `orEmpty factory method returns Empty if provided contexts empty`() {
    val result = Multi.orEmpty(listOf())
    assertEquals(Empty, result)
  }

  @Test
  fun `orEmpty factory method returns Empty if provided contexts not empty`() {
    val input = listOf(Connection("connection"))
    val result = Multi.orEmpty(input)
    assertEquals(Multi(input), result)
  }
}

class WorkspaceTest {
  @Test
  fun `verify data`() {
    Workspace("workspace key").also {
      assertEquals("workspace", it.kind)
      assertEquals("workspace key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }
}

class UserTest {
  @Test
  fun `verify data`() {
    User("user key").also {
      assertEquals("user", it.kind)
      assertEquals("user key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }

  @Test
  fun `verify data with email`() {
    val userId = UUID.randomUUID()
    User(userId, EmailAttribute("user@airbyte.io")).also {
      assertEquals("user", it.kind)
      assertEquals(userId.toString(), it.key)

      val emailAttr = it.attrs.firstOrNull()
      assertEquals("email", emailAttr?.key)
      assertEquals("user@airbyte.io", emailAttr?.value)
      assertEquals(true, emailAttr?.private)
    }
  }
}

class ConnectionTest {
  @Test
  fun `verify data`() {
    Connection("connection key").also {
      assertEquals("connection", it.kind)
      assertEquals("connection key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }
}

class SourceTest {
  @Test
  fun `verify data`() {
    Source("source key").also {
      assertEquals("source", it.kind)
      assertEquals("source key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }
}

class DestinationTest {
  @Test
  fun `verify data`() {
    Destination("destination key").also {
      assertEquals("destination", it.kind)
      assertEquals("destination key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }
}

class SourceDefinitionTest {
  @Test
  fun `verify data`() {
    SourceDefinition("source definition key").also {
      assertEquals("source-definition", it.kind)
      assertEquals("source definition key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }
}

class DestinationDefinitionTest {
  @Test
  fun `verify data`() {
    DestinationDefinition("destination definition key").also {
      assertEquals("destination-definition", it.kind)
      assertEquals("destination definition key", it.key)
      assertTrue(it.attrs.isEmpty())
    }
  }
}

class MonoidTests {
  @MethodSource("mergeMatrix")
  @ParameterizedTest
  fun `merge combines two Context (associative binary operation)`(
    a: Context,
    b: Context,
    expected: Context,
  ) {
    val result = a.merge(b)
    assertEquals(expected, result)
  }

  @MethodSource("identityMatrix")
  @ParameterizedTest
  fun `Merging Empty is a noop (identity) `(a: Context) {
    val result1 = Empty.merge(a)
    assertEquals(a, result1)

    val result2 = a.merge(Empty)
    assertEquals(a, result2)
  }

  companion object {
    @JvmStatic
    fun mergeMatrix() =
      listOf(
        Arguments.of(Workspace(""), Connection(""), Multi(listOf(Workspace(""), Connection("")))),
        Arguments.of(Workspace(""), Organization(""), Multi(listOf(Workspace(""), Organization("")))),
        Arguments.of(Destination(""), SourceDefinition(""), Multi(listOf(Destination(""), SourceDefinition("")))),
        Arguments.of(Source(""), DestinationDefinition(""), Multi(listOf(Source(""), DestinationDefinition("")))),
        Arguments.of(Source(""), Source(""), Source("")),
        Arguments.of(Connection(""), Connection(""), Connection("")),
        Arguments.of(Workspace(""), Workspace(""), Workspace("")),
        Arguments.of(
          Multi(listOf(Workspace(""), Connection(""))),
          Multi(listOf(Organization(""), Source(""))),
          Multi(listOf(Workspace(""), Connection(""), Organization(""), Source(""))),
        ),
        Arguments.of(
          Multi(listOf(Organization(""))),
          Multi(listOf(Workspace(""), Source(""))),
          Multi(listOf(Organization(""), Workspace(""), Source(""))),
        ),
      )

    @JvmStatic
    fun identityMatrix() =
      listOf(
        Arguments.of(Empty),
        Arguments.of(Workspace(UUID.randomUUID())),
        Arguments.of(Connection(UUID.randomUUID())),
        Arguments.of(Organization(UUID.randomUUID())),
        Arguments.of(Source(UUID.randomUUID())),
        Arguments.of(SourceDefinition(UUID.randomUUID())),
        Arguments.of(Destination(UUID.randomUUID())),
        Arguments.of(DestinationDefinition(UUID.randomUUID())),
        Arguments.of(Multi(listOf(Workspace(UUID.randomUUID()), Connection(UUID.randomUUID()), Organization(UUID.randomUUID())))),
        Arguments.of(Multi(listOf(Organization(UUID.randomUUID())))),
        Arguments.of(Multi(listOf(Organization(UUID.randomUUID()), Source(UUID.randomUUID())))),
      )
  }
}
