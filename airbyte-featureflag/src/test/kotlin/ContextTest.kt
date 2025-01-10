/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.featureflag

import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MultiTest {
  @Test
  fun `verify data`() {
    val source = Source("source")
    val workspace = Workspace("workspace")

    Multi(listOf(source, workspace)).also {
      assert(it.kind == "multi")
    }
  }

  @Test
  fun `Multi cannot contain another Multi`() {
    val source = Source("source")
    val workspace = Workspace("workspace")
    val multi = Multi(listOf(source, workspace))

    assertFailsWith<IllegalArgumentException> {
      Multi(listOf(source, workspace, multi))
    }
  }

  @Test
  fun `fetchContexts returns correct results`() {
    val source1 = Source("source1")
    val source2 = Source("source2")
    val workspace = Workspace("workspace")
    val multi = Multi(listOf(source1, source2, workspace))

    assertEquals(listOf(source1, source2), multi.fetchContexts(), "two source contexts")
    assertEquals(listOf(workspace), multi.fetchContexts(), "one workspace context")
    assertEquals(listOf(), multi.fetchContexts<Connection>(), "should be empty as no connection context was provided")
  }

  @Test
  fun `key is an empty string`() {
    val workspace = Workspace("workspace")
    val connection = Connection("connection")
    val source = Source("source")
    val destination = Destination("destination")
    val user = User("user")

    Multi(listOf(user, destination, source, connection, workspace)).also {
      assert(it.key.isEmpty())
      assert(it.attrs.isEmpty())
    }
  }

  @Test
  fun `no contexts is an exception`() {
    assertFailsWith<IllegalArgumentException> {
      Multi(listOf())
    }
  }
}

class WorkspaceTest {
  @Test
  fun `verify data`() {
    Workspace("workspace key").also {
      assert(it.kind == "workspace")
      assert(it.key == "workspace key")
      assert(it.attrs.isEmpty())
    }
  }
}

class UserTest {
  @Test
  fun `verify data`() {
    User("user key").also {
      assert(it.kind == "user")
      assert(it.key == "user key")
      assert(it.attrs.isEmpty())
    }
  }

  @Test
  fun `verify data with email`() {
    val userId = UUID.randomUUID()
    User(userId, EmailAttribute("user@airbyte.io")).also {
      assert(it.kind == "user")
      assert(it.key == userId.toString())

      val emailAttr = it.attrs.firstOrNull()
      assert(emailAttr?.key == "email")
      assert(emailAttr?.value == "user@airbyte.io")
      assert(emailAttr?.private == true)
    }
  }
}

class ConnectionTest {
  @Test
  fun `verify data`() {
    Connection("connection key").also {
      assert(it.kind == "connection")
      assert(it.key == "connection key")
      assert(it.attrs.isEmpty())
    }
  }
}

class SourceTest {
  @Test
  fun `verify data`() {
    Source("source key").also {
      assert(it.kind == "source")
      assert(it.key == "source key")
      assert(it.attrs.isEmpty())
    }
  }
}

class DestinationTest {
  @Test
  fun `verify data`() {
    Destination("destination key").also {
      assert(it.kind == "destination")
      assert(it.key == "destination key")
      assert(it.attrs.isEmpty())
    }
  }
}

class SourceDefinitionTest {
  @Test
  fun `verify data`() {
    SourceDefinition("source definition key").also {
      assert(it.kind == "source-definition")
      assert(it.key == "source definition key")
      assert(it.attrs.isEmpty())
    }
  }
}

class DestinationDefinitionTest {
  @Test
  fun `verify data`() {
    DestinationDefinition("destination definition key").also {
      assert(it.kind == "destination-definition")
      assert(it.key == "destination definition key")
      assert(it.attrs.isEmpty())
    }
  }
}
