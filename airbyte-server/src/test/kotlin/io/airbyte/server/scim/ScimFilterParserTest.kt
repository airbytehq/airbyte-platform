/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class ScimFilterParserTest {
  @Test
  fun `parses every supported User equality shape and and clauses`() {
    assertThat(ScimFilterParser.parseUser("userName eq \"Alice@Example.com\""))
      .isEqualTo(ScimFilter.And(listOf(ScimFilter.Equal(ScimFilter.Attribute.USER_NAME, "Alice@Example.com", false))))
    assertThat(ScimFilterParser.parseUser("externalId eq \"Case-Exact\""))
      .isEqualTo(ScimFilter.And(listOf(ScimFilter.Equal(ScimFilter.Attribute.EXTERNAL_ID, "Case-Exact", true))))
    assertThat(ScimFilterParser.parseUser("emails.value eq \"alice@example.com\""))
      .isEqualTo(ScimFilter.And(listOf(ScimFilter.Equal(ScimFilter.Attribute.EMAIL, "alice@example.com", false))))
    assertThat(ScimFilterParser.parseUser("emails[type eq \"work\"].value eq \"alice@example.com\""))
      .isEqualTo(ScimFilter.And(listOf(ScimFilter.Equal(ScimFilter.Attribute.WORK_EMAIL, "alice@example.com", false))))

    assertThat(
      ScimFilterParser.parseUser(
        "USERname EQ \"Alice@Example.com\" AnD externalId eq \"Case-Exact\" and emails.value eq \"other@example.com\"",
      ),
    ).isEqualTo(
      ScimFilter.And(
        listOf(
          ScimFilter.Equal(ScimFilter.Attribute.USER_NAME, "Alice@Example.com", false),
          ScimFilter.Equal(ScimFilter.Attribute.EXTERNAL_ID, "Case-Exact", true),
          ScimFilter.Equal(ScimFilter.Attribute.EMAIL, "other@example.com", false),
        ),
      ),
    )
  }

  @Test
  fun `parses every supported Group filter shape`() {
    assertThat(ScimFilterParser.parseGroup("displayName eq \"Engineering\""))
      .isEqualTo(ScimFilter.Equal(ScimFilter.Attribute.DISPLAY_NAME, "Engineering", false))
    assertThat(ScimFilterParser.parseGroup("members[value eq \"mapping-id\"]"))
      .isEqualTo(ScimFilter.Equal(ScimFilter.Attribute.MEMBER, "mapping-id", true))
  }

  @Test
  fun `decodes escaped string values without weakening the grammar`() {
    assertThat(ScimFilterParser.parseUser("externalId eq \"a\\\\b\\\"c\""))
      .isEqualTo(ScimFilter.And(listOf(ScimFilter.Equal(ScimFilter.Attribute.EXTERNAL_ID, "a\\b\"c", true))))
  }

  @Test
  fun `filter attribute paths reject Unicode lookalikes as invalidFilter`() {
    listOf(
      "uſerName eq \"alice@example.com\"",
      "urn:ietf:paramſ:scim:schemas:core:2.0:User:userName eq \"alice@example.com\"",
    ).forEach { filter ->
      val exception = assertThrows<ScimException>(filter) { ScimFilterParser.parseUser(filter) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidFilter")
    }
  }

  @Test
  fun `filter attribute paths accept mixed canonical ASCII case`() {
    assertThat(
      ScimFilterParser.parseUser(
        "URN:IETF:PARAMS:SCIM:SCHEMAS:CORE:2.0:USER:USERNAME EQ \"Alice@Example.com\"",
      ),
    ).isEqualTo(
      ScimFilter.And(listOf(ScimFilter.Equal(ScimFilter.Attribute.USER_NAME, "Alice@Example.com", false))),
    )
  }

  @Test
  fun `malformed and unsupported filters fail closed as invalidFilter`() {
    listOf(
      "",
      "   ",
      "userName eq",
      "userName co \"alice\"",
      "userName eq \"alice\" or externalId eq \"id\"",
      "unknown eq \"value\"",
      "emails[type eq \"home\"].value eq \"alice@example.com\"",
      "displayName eq \"one\" and displayName eq \"two\"",
      "members[value eq \"id\"].value",
      "members[value ne \"id\"]",
    ).forEach { filter ->
      val exception =
        assertThrows<ScimException>(filter) {
          if (filter.startsWith("displayName") || filter.startsWith("members")) {
            ScimFilterParser.parseGroup(filter)
          } else {
            ScimFilterParser.parseUser(filter)
          }
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidFilter")
    }
  }

  @Test
  fun `non-normative filter whitespace and separators fail closed as invalidFilter`() {
    listOf(
      " userName eq \"alice\"",
      "userName eq \"alice\" ",
      "userName  eq \"alice\"",
      "userName eq  \"alice\"",
      "userName\teq \"alice\"",
      "emails .value eq \"alice@example.com\"",
      "emails. value eq \"alice@example.com\"",
      "emails [type eq \"work\"].value eq \"alice@example.com\"",
      "emails[ type eq \"work\"].value eq \"alice@example.com\"",
      "emails[type  eq \"work\"].value eq \"alice@example.com\"",
      "emails[type eq  \"work\"].value eq \"alice@example.com\"",
      "emails[type eq \"work\" ].value eq \"alice@example.com\"",
      "emails[type eq \"work\"] .value eq \"alice@example.com\"",
      "userName eq \"alice\"  and externalId eq \"employee-1\"",
      "userName eq \"alice\" and  externalId eq \"employee-1\"",
    ).forEach { filter ->
      val exception = assertThrows<ScimException>(filter) { ScimFilterParser.parseUser(filter) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidFilter")
    }

    listOf(
      " displayName eq \"Engineering\"",
      "displayName eq \"Engineering\" ",
      "members [value eq \"mapping-id\"]",
      "members[ value eq \"mapping-id\"]",
      "members[value  eq \"mapping-id\"]",
      "members[value eq  \"mapping-id\"]",
      "members[value eq \"mapping-id\" ]",
    ).forEach { filter ->
      val exception = assertThrows<ScimException>(filter) { ScimFilterParser.parseGroup(filter) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidFilter")
    }
  }
}
