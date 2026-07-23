/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micronaut.http.HttpStatus
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ScimPatchProcessorTest {
  private val objectMapper = jacksonObjectMapper()

  @Test
  fun `mixed-case User operations apply sequentially to one independent representation`() {
    val original = user()
    val request =
      patch(
        """
        [
          {"op":"Add","value":{"displayName":"First","password":"discard","id":"ignored"}},
          {"op":"Replace","path":"displayName","value":"Second"},
          {"op":"rEpLaCe","path":"name.givenName","value":"Alice"},
          {"op":"REPLACE","path":"emails[type eq \"work\"].value","value":"new@example.com"},
          {"op":"remove","path":"externalId"}
        ]
        """.trimIndent(),
      )

    val result = ScimPatchProcessor.applyUser(original, request).resource

    assertThat(result["displayName"].asText()).isEqualTo("Second")
    assertThat(result["name"]["givenName"].asText()).isEqualTo("Alice")
    assertThat(result["emails"][0]["value"].asText()).isEqualTo("new@example.com")
    assertThat(result.has("externalId")).isFalse()
    assertThat(result.has("password")).isFalse()
    assertThat(result["id"].asText()).isEqualTo("user-id")
    assertThat(original).isEqualTo(user())
  }

  @Test
  fun `pathless User replace supports mutable objects without clearing omitted fields`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"replace","value":{"active":false,"name":{"familyName":"Updated"}}}]"""),
        ).resource

    assertThat(result["active"].asBoolean()).isFalse()
    assertThat(result["userName"].asText()).isEqualTo("alice@example.com")
    assertThat(result["name"]["familyName"].asText()).isEqualTo("Updated")
    assertThat(result["name"]["givenName"].asText()).isEqualTo("Original")
    assertThat(result["externalId"].asText()).isEqualTo("external")
  }

  @Test
  fun `User PATCH exposes ordered active transitions while preserving atomic input`() {
    val original = user()

    val result =
      ScimPatchProcessor.applyUser(
        original,
        patch(
          """[{"op":"replace","path":"active","value":false},{"op":"replace","path":"active","value":true}]""",
        ),
      )

    assertThat(result.resource["active"].asBoolean()).isTrue()
    assertThat(result.activeTransitions).containsExactly(
      ScimUserActiveTransition(from = true, to = false),
      ScimUserActiveTransition(from = false, to = true),
    )
    assertThat(original).isEqualTo(user())
  }

  @Test
  fun `User PATCH remove active restores the default and exposes a reactivation transition`() {
    val original = user().put("active", false)

    val result =
      ScimPatchProcessor.applyUser(
        original,
        patch("""[{"op":"remove","path":"active"}]"""),
      )

    assertThat(result.resource).isEqualTo(user())
    assertThat(result.activeTransitions).containsExactly(
      ScimUserActiveTransition(from = false, to = true),
    )
    assertThat(original).isEqualTo(user().put("active", false))
  }

  @Test
  fun `pathless User replace accepts nonempty objects containing only recognized no-op fields`() {
    listOf(
      """{"password":"discarded"}""",
      """{"id":"ignored","meta":{"resourceType":"User","created":"2026-07-17T00:00:00Z","lastModified":"2026-07-17T01:00:00Z","location":"https://example.com/scim/v2/Users/ignored"},"groups":[{"value":"ignored","${'$'}ref":"https://example.com/scim/v2/Groups/ignored","display":"Ignored group"}]}""",
      """{"schemas":["$SCIM_USER_SCHEMA"]}""",
    ).forEach { value ->
      val original = user()

      val result = ScimPatchProcessor.applyUser(original, patch("""[{"op":"replace","value":$value}]""")).resource

      assertThat(result).isEqualTo(original)
    }
  }

  @Test
  fun `direct User name replace preserves omitted subattributes`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"replace","path":"name","value":{"familyName":"Updated"}}]"""),
        ).resource

    assertThat(result["name"]["familyName"].asText()).isEqualTo("Updated")
    assertThat(result["name"]["givenName"].asText()).isEqualTo("Original")
  }

  @Test
  fun `direct User emails add appends a single complex object`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"add","path":"emails","value":{"value":" alias@example.com ","type":"home"}}]"""),
        ).resource

    assertThat(result["emails"]).hasSize(2)
    assertThat(result["emails"][1]).isEqualTo(json("""{"value":"alias@example.com","type":"home"}"""))
  }

  @Test
  fun `direct User emails add preserves empty and multi-entry array behavior`() {
    val unchanged =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"add","path":"emails","value":[]}]"""),
        ).resource
    val appended =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch(
            """[{"op":"add","path":"emails","value":[{"value":"home@example.com","type":"home"},{"value":"other@example.com","type":"other"}]}]""",
          ),
        ).resource

    assertThat(unchanged).isEqualTo(user())
    assertThat(appended["emails"].map { it["value"].asText() })
      .containsExactly("alice@example.com", "home@example.com", "other@example.com")
  }

  @Test
  fun `direct User emails add promotes a submitted primary and demotes the existing primary`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"add","path":"emails","value":{"value":"new@example.com","type":"home","primary":true}}]"""),
        ).resource

    assertThat(result["emails"]).isEqualTo(
      json(
        """
        {
          "emails":[
            {"value":"alice@example.com","type":"work","primary":false},
            {"value":"new@example.com","type":"home","primary":true}
          ]
        }
        """.trimIndent(),
      )["emails"],
    )
  }

  @Test
  fun `pathless User emails add promotes a submitted primary and demotes the existing primary`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"add","value":{"emails":[{"value":"new@example.com","type":"home","primary":true}]}}]"""),
        ).resource

    assertThat(result["emails"]).isEqualTo(
      json(
        """
        {
          "emails":[
            {"value":"alice@example.com","type":"work","primary":false},
            {"value":"new@example.com","type":"home","primary":true}
          ]
        }
        """.trimIndent(),
      )["emails"],
    )
  }

  @Test
  fun `User emails add rejects multiple submitted primaries atomically`() {
    assertInvalidEmailPatchIsAtomic(
      """[{"op":"add","path":"emails","value":[{"value":"first@example.com","primary":true},{"value":"second@example.com","primary":true}]}]""",
    )
  }

  @Test
  fun `filtered User email replace without a matching work entry returns noTarget atomically`() {
    val original =
      user().also {
        it
          .putArray("emails")
          .addObject()
          .put("value", "alice@example.com")
          .put("type", "home")
          .put("primary", true)
      }
    val before = original.deepCopy()

    val exception =
      assertThrows<ScimException> {
        ScimPatchProcessor.applyUser(
          original,
          patch("""[{"op":"replace","path":"emails[type eq \"work\"].value","value":"new@example.com"}]"""),
        )
      }

    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo("noTarget")
    assertThat(original).isEqualTo(before)
  }

  @Test
  fun `direct and pathless User email add and replace discard textual display enrichment`() {
    listOf(
      """{"op":"add","path":"emails","value":{"value":"direct-add@example.com","type":"home","display":"Direct add"}}""",
      """{"op":"replace","path":"emails","value":[{"value":"direct-replace@example.com","type":"work","display":"Direct replace"}]}""",
      """{"op":"add","value":{"emails":[{"value":"pathless-add@example.com","type":"home","display":"Pathless add"}]}}""",
      """{"op":"replace","value":{"emails":[{"value":"pathless-replace@example.com","type":"work","display":"Pathless replace"}]}}""",
    ).forEach { operation ->
      val result = ScimPatchProcessor.applyUser(user(), patch("[$operation]")).resource

      assertThat(result["emails"]).allSatisfy { email -> assertThat(email.has("display")).isFalse() }
    }
  }

  @Test
  fun `direct and pathless User email add and replace reject non-string display atomically`() {
    listOf(
      """{"op":"add","path":"emails","value":{"value":"direct-add@example.com","type":"home","display":false}}""",
      """{"op":"replace","path":"emails","value":[{"value":"direct-replace@example.com","type":"work","display":false}]}""",
      """{"op":"add","value":{"emails":[{"value":"pathless-add@example.com","type":"home","display":false}]}}""",
      """{"op":"replace","value":{"emails":[{"value":"pathless-replace@example.com","type":"work","display":false}]}}""",
    ).forEach { operation ->
      val original = user()
      val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(original, patch("[$operation]")) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(user())
    }
  }

  @Test
  fun `direct User emails add rejects null and empty complex values atomically`() {
    listOf(
      "null" to "invalidValue",
      "{}" to "invalidValue",
    ).forEach { (value, expectedScimType) ->
      val original = user()
      val exception =
        assertThrows<ScimException> {
          ScimPatchProcessor.applyUser(
            original,
            patch("""[{"op":"add","path":"emails","value":$value}]"""),
          )
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo(expectedScimType)
      assertThat(original).isEqualTo(user())
    }
  }

  @Test
  fun `User PATCH rejects case-insensitive duplicate pathless attributes and direct or pathless subattributes atomically`() {
    listOf(
      """{"op":"add","value":{"userName":"first@example.com","USERNAME":"second@example.com"}}""",
      """{"op":"replace","value":{"displayName":"First","DISPLAYNAME":"Second"}}""",
      """{"op":"add","path":"name","value":{"givenName":"First","GIVENNAME":"Second"}}""",
      """{"op":"replace","path":"name","value":{"familyName":"First","FAMILYNAME":"Second"}}""",
      """{"op":"add","value":{"name":{"givenName":"First","GIVENNAME":"Second"}}}""",
      """{"op":"replace","value":{"name":{"familyName":"First","FAMILYNAME":"Second"}}}""",
      """{"op":"add","path":"emails","value":{"value":"first@example.com","VALUE":"second@example.com","type":"home"}}""",
      """{"op":"replace","path":"emails","value":[{"value":"first@example.com","VALUE":"second@example.com","type":"work"}]}""",
      """{"op":"add","value":{"emails":[{"value":"first@example.com","VALUE":"second@example.com","type":"home"}]}}""",
      """{"op":"replace","value":{"emails":[{"value":"first@example.com","VALUE":"second@example.com","type":"work"}]}}""",
    ).forEach { operation ->
      val original = user()
      val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(original, patch("[$operation]")) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(user())
    }
  }

  @Test
  fun `pathless User remove returns noTarget`() {
    assertScimError("noTarget") {
      ScimPatchProcessor.applyUser(user(), patch("""[{"op":"remove"}]"""))
    }
  }

  @Test
  fun `pathless User remove prevalidation returns noTarget`() {
    assertScimError("noTarget") {
      ScimPatchProcessor.validateUserPatch(patch("""[{"op":"remove"}]"""))
    }
  }

  @Test
  fun `case-insensitive field variants remain sequential across separate pathless User operations`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch(
            """[{"op":"replace","value":{"userName":"first@example.com","name":{"givenName":"First"},"emails":[{"value":"first@example.com","type":"work"}]}},{"op":"replace","value":{"USERNAME":"second@example.com","NAME":{"GIVENNAME":"Second"},"EMAILS":[{"VALUE":"second@example.com","TYPE":"work"}]}}]""",
          ),
        ).resource

    assertThat(result["userName"].asText()).isEqualTo("second@example.com")
    assertThat(result["name"]["givenName"].asText()).isEqualTo("Second")
    assertThat(result["emails"].single()["value"].asText()).isEqualTo("second@example.com")
  }

  @Test
  fun `pathless Group remove returns noTarget`() {
    assertScimError("noTarget") {
      ScimPatchProcessor.applyGroup(
        group(),
        patch("""[{"op":"remove"}]"""),
        TENANT,
        acceptingValidator(),
      )
    }
  }

  @Test
  fun `direct User emails mutation rejects a malformed address atomically`() {
    assertInvalidEmailPatchIsAtomic(
      """[{"op":"replace","path":"emails","value":[{"value":"not-an-email","type":"work"}]}]""",
    )
  }

  @Test
  fun `pathless User emails mutation rejects a malformed address atomically`() {
    assertInvalidEmailPatchIsAtomic(
      """[{"op":"replace","value":{"emails":[{"value":"not-an-email","type":"work"}]}}]""",
    )
  }

  @Test
  fun `User work-email value-path mutation rejects a malformed address atomically`() {
    assertInvalidEmailPatchIsAtomic(
      """[{"op":"replace","path":"emails[type eq \"work\"].value","value":" not-an-email "}]""",
    )
  }

  @Test
  fun `User and Group add or replace require values and reject null outside externalId`() {
    listOf<() -> Unit>(
      { ScimPatchProcessor.applyUser(user(), patch("""[{"op":"add","path":"displayName"}]""")) },
      { ScimPatchProcessor.applyUser(user(), patch("""[{"op":"replace","path":"externalId"}]""")) },
      { ScimPatchProcessor.applyUser(user(), patch("""[{"op":"replace","path":"active","value":null}]""")) },
      { ScimPatchProcessor.applyUser(user(), patch("""[{"op":"replace","path":"name.givenName","value":null}]""")) },
      { ScimPatchProcessor.applyUser(user(), patch("""[{"op":"replace","value":{"active":null}}]""")) },
      {
        ScimPatchProcessor.applyGroup(
          group(),
          patch("""[{"op":"replace","path":"externalId"}]"""),
          TENANT,
          acceptingValidator(),
        )
      },
    ).forEach { operation -> assertScimError("invalidValue", operation) }
  }

  @Test
  fun `explicit null clears only User and Group externalId`() {
    val userResult =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch("""[{"op":"replace","path":"externalId","value":null}]"""),
        ).resource
    val groupWithExternalId = group().put("externalId", "group-external")
    val groupResult =
      ScimPatchProcessor.applyGroup(
        groupWithExternalId,
        patch("""[{"op":"replace","value":{"externalId":null}}]"""),
        TENANT,
        acceptingValidator(),
      )

    assertThat(userResult.has("externalId")).isFalse()
    assertThat(groupResult.has("externalId")).isFalse()
  }

  @Test
  fun `final User validation rejects null empty and unselectable required values atomically`() {
    listOf(
      """[{"op":"replace","path":"userName","value":null}]""",
      """[{"op":"replace","path":"userName","value":""}]""",
      """[{"op":"remove","path":"emails"}]""",
      """[{"op":"replace","path":"emails","value":[]}]""",
      """[{"op":"replace","path":"emails","value":[{"value":"a@example.com","type":"home"}]}]""",
      """[{"op":"replace","path":"emails","value":[{"value":"a@example.com","primary":true},{"value":"b@example.com","primary":true}]}]""",
    ).forEach { operations ->
      val original = user()
      val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(original, patch(operations)) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(user())
    }
  }

  @Test
  fun `User PATCH validates profile URL and email invariants after every operation`() {
    listOf(
      """[{"op":"replace","path":"profileUrl","value":"https://example .com/alice"}]""",
      """[{"op":"replace","value":{"profileUrl":"https://example .com/alice"}}]""",
      """[{"op":"replace","path":"emails","value":[{"value":"not-an-email","type":"work"}]}]""",
      """[{"op":"replace","path":"emails","value":[{"value":"alice@example.com","type":"work"},{"value":" ALICE@EXAMPLE.COM ","type":"home"}]}]""",
      """[{"op":"replace","path":"emails","value":[{"value":"alice@example.com","type":"work"},{"value":" ALICE@EXAMPLE.COM ","type":"home"}]},{"op":"replace","path":"emails","value":[{"value":"restored@example.com","type":"work"}]}]""",
    ).forEach { operations ->
      val original = user()
      val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(original, patch(operations)) }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(user())
    }
  }

  @Test
  fun `User PATCH rejects malformed RFC 5321 email values after each operation`() {
    listOf(
      "alice@.example.com",
      "alice@example..com",
      ".alice@example.com",
      "alice.@example.com",
      "alice..smith@example.com",
      "alice@-example.com",
      "alice@example-.com",
    ).forEach { email ->
      val original = user()
      val escapedEmail = objectMapper.writeValueAsString(email)
      val exception =
        assertThrows<ScimException> {
          ScimPatchProcessor.applyUser(
            original,
            patch(
              """[{"op":"replace","path":"emails","value":[{"value":"valid@example.com","type":"work","primary":true},{"value":$escapedEmail,"type":"home"}]},{"op":"replace","path":"emails","value":[{"value":"restored@example.com","type":"work"}]}]""",
            ),
          )
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(user())
    }
  }

  @Test
  fun `User PATCH accepts valid RFC 5321 email edge cases`() {
    listOf(
      "alice+tag@example.travel",
      "\"alice smith\"@example.com",
      "alice@[192.0.2.1]",
    ).forEach { email ->
      val escapedEmail = objectMapper.writeValueAsString(email)
      val result =
        ScimPatchProcessor
          .applyUser(
            user(),
            patch("""[{"op":"replace","path":"emails[type eq \"work\"].value","value":$escapedEmail}]"""),
          ).resource

      assertThat(
        result
          .path("emails")
          .path(0)
          .path("value")
          .asText(),
      ).isEqualTo(email)
    }
  }

  @Test
  fun `User PATCH profile URL requires a hierarchical HTTP or HTTPS URI with a host`() {
    listOf(
      "urn:example:user:alice",
      "mailto:alice@example.com",
      "file:///profiles/alice",
      "https:profile",
      "https:///profiles/alice",
      "//example.com/profiles/alice",
      "ftp://example.com/profiles/alice",
    ).forEach { profileUrl ->
      val original = user()
      val exception =
        assertThrows<ScimException> {
          ScimPatchProcessor.applyUser(
            original,
            patch(
              """[{"op":"replace","path":"profileUrl","value":"$profileUrl"},{"op":"replace","path":"profileUrl","value":"https://example.com/restored"}]""",
            ),
          )
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(user())
    }

    listOf(
      "http://example.com/profiles/alice",
      "https://profiles.example.com:8443/users/alice?view=full#bio",
    ).forEach { profileUrl ->
      val result =
        ScimPatchProcessor
          .applyUser(
            user(),
            patch("""[{"op":"replace","path":"profileUrl","value":"$profileUrl"}]"""),
          ).resource

      assertThat(result.path("profileUrl").asText()).isEqualTo(profileUrl)
    }
  }

  @Test
  fun `User PATCH rejects removing then restoring userName atomically`() {
    val original = user()

    val exception =
      assertThrows<ScimException> {
        ScimPatchProcessor.applyUser(
          original,
          patch(
            """[{"op":"remove","path":"userName"},{"op":"add","path":"userName","value":"restored@example.com"}]""",
          ),
        )
      }

    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo("invalidValue")
    assertThat(original).isEqualTo(user())
  }

  @Test
  fun `User PATCH rejects removing then restoring selectable emails atomically`() {
    val original = user()

    val exception =
      assertThrows<ScimException> {
        ScimPatchProcessor.applyUser(
          original,
          patch(
            """[{"op":"remove","path":"emails"},{"op":"replace","path":"emails","value":[{"value":"restored@example.com","type":"work"}]}]""",
          ),
        )
      }

    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo("invalidValue")
    assertThat(original).isEqualTo(user())
  }

  @Test
  fun `Group PATCH rejects removing then restoring displayName atomically`() {
    val original = group()

    val exception =
      assertThrows<ScimException> {
        ScimPatchProcessor.applyGroup(
          original,
          patch(
            """[{"op":"remove","path":"displayName"},{"op":"add","path":"displayName","value":"Restored"}]""",
          ),
          TENANT,
          acceptingValidator(),
        )
      }

    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo("invalidValue")
    assertThat(original).isEqualTo(group())
  }

  @Test
  fun `PatchOp schema and a nonempty operations array are required`() {
    listOf(
      "{}",
      """{"schemas":null,"Operations":[]}""",
      """{"schemas":[],"Operations":[]}""",
      """{"schemas":["wrong"],"Operations":[{"op":"add","value":{}}]}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":null}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[]}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"merge","value":{}}]}""",
    ).forEach { requestJson ->
      val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(user(), json(requestJson)) }
      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
    }
  }

  @Test
  fun `PatchOp field names accept mixed canonical ASCII case`() {
    val request =
      json(
        """
        {
          "ScHeMaS":["$SCIM_PATCH_OP_SCHEMA"],
          "oPeRaTiOnS":[{"OP":"rEpLaCe","PaTh":"displayName","VaLuE":"Updated"}]
        }
        """.trimIndent(),
      )

    val result = ScimPatchProcessor.applyUser(user(), request).resource

    assertThat(result["displayName"].asText()).isEqualTo("Updated")
  }

  @Test
  fun `PatchOp rejects ambiguous duplicate field-name case variants`() {
    listOf(
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"SCHEMAS":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Updated"}]}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Updated"}],"OPERATIONS":[{"op":"replace","path":"displayName","value":"Updated"}]}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","OP":"replace","path":"displayName","value":"Updated"}]}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","PATH":"displayName","value":"Updated"}]}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Updated","VALUE":"Updated"}]}""",
    ).forEach { requestJson ->
      assertScimError("invalidValue") { ScimPatchProcessor.applyUser(user(), json(requestJson)) }
    }
  }

  @Test
  fun `nested SCIM objects reject ambiguous duplicate field-name case variants`() {
    listOf<() -> Unit>(
      {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","value":{"displayName":"First","DISPLAYNAME":"Second"}}]"""),
        )
      },
      {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","path":"name","value":{"givenName":"First","GIVENNAME":"Second"}}]"""),
        )
      },
      {
        ScimPatchProcessor.applyUser(
          user(),
          patch(
            """[{"op":"replace","path":"emails","value":[{"value":"first@example.com","VALUE":"second@example.com","type":"work"}]}]""",
          ),
        )
      },
      {
        ScimPatchProcessor.applyGroup(
          group(),
          patch("""[{"op":"replace","value":{"displayName":"First","DISPLAYNAME":"Second"}}]"""),
          TENANT,
          acceptingValidator(),
        )
      },
      {
        ScimPatchProcessor.applyGroup(
          group(),
          patch("""[{"op":"replace","path":"members","value":[{"value":"one","VALUE":"two"}]}]"""),
          TENANT,
          acceptingValidator(),
        )
      },
    ).forEach { operation -> assertScimError("invalidValue", operation) }
  }

  @Test
  fun `PatchOp remains fail-closed for unknown field names`() {
    listOf(
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Updated"}],"extra":true}""",
      """{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Updated","extra":true}]}""",
      """{"ſchemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":[{"op":"replace","path":"displayName","value":"Updated"}]}""",
    ).forEach { requestJson ->
      assertScimError("invalidValue") { ScimPatchProcessor.applyUser(user(), json(requestJson)) }
    }
  }

  @Test
  fun `unsupported paths malformed value filters and immutable member values use exact SCIM errors`() {
    assertScimError("invalidPath") {
      ScimPatchProcessor.applyUser(user(), patch("""[{"op":"replace","path":"enterprise.department","value":"x"}]"""))
    }
    assertScimError("invalidFilter") {
      ScimPatchProcessor.applyUser(user(), patch("""[{"op":"replace","path":"emails[type ne \"work\"].value","value":"x"}]"""))
    }
    listOf(
      """members[type eq "User"].value""",
      """members[value ne "one"].value""",
      """members[value eq].value""",
    ).forEach { path ->
      val escapedPath = path.replace("\"", "\\\"")
      assertScimError("invalidFilter") {
        ScimPatchProcessor.applyGroup(
          group(),
          patch("""[{"op":"replace","path":"$escapedPath","value":"two"}]"""),
          TENANT,
          acceptingValidator(),
        )
      }
    }
    assertScimError("mutability") {
      ScimPatchProcessor.applyGroup(
        group(),
        patch("""[{"op":"replace","path":"members[value eq \"one\"].value","value":"two"}]"""),
        TENANT,
        acceptingValidator(),
      )
    }
  }

  @Test
  fun `unsupported PATCH value-path roots and extensions are invalidPath`() {
    listOf(
      """urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:emails[type eq "work"].value""",
      """addresses[type eq "work"].value""",
    ).forEach { path ->
      val escapedPath = path.replace("\"", "\\\"")
      assertScimError("invalidPath") {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","path":"$escapedPath","value":"updated@example.com"}]"""),
        )
      }
    }

    assertScimError("invalidPath") {
      ScimPatchProcessor.applyGroup(
        group(),
        patch("""[{"op":"remove","path":"users[value eq \"one\"]"}]"""),
        TENANT,
        acceptingValidator(),
      )
    }
  }

  @Test
  fun `PATCH paths reject Unicode lookalikes with exact SCIM errors`() {
    listOf(
      "uſerName" to "invalidPath",
      "urn:ietf:paramſ:scim:schemas:core:2.0:User:displayName" to "invalidPath",
      "emailſ[type eq \"work\"].value" to "invalidPath",
    ).forEach { (path, scimType) ->
      val escapedPath = path.replace("\"", "\\\"")
      assertScimError(scimType) {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","path":"$escapedPath","value":"Updated"}]"""),
        )
      }
    }

    assertScimError("invalidPath") {
      ScimPatchProcessor.applyGroup(
        group(),
        patch("""[{"op":"remove","path":"memberſ[value eq \"one\"]"}]"""),
        TENANT,
        acceptingValidator(),
      )
    }
  }

  @Test
  fun `PATCH paths accept mixed canonical ASCII case`() {
    val result =
      ScimPatchProcessor
        .applyUser(
          user(),
          patch(
            """[{"op":"rEpLaCe","path":"URN:IETF:PARAMS:SCIM:SCHEMAS:CORE:2.0:USER:DISPLAYNAME","value":"Updated"}]""",
          ),
        ).resource

    assertThat(result["displayName"].asText()).isEqualTo("Updated")
  }

  @Test
  fun `exact ignored User paths remain no-ops while malformed descendants are invalidPath`() {
    listOf(
      "id" to objectMapper.readTree("\"ignored\""),
      "meta" to
        objectMapper.readTree(
          """{"resourceType":"User","created":"2026-07-17T00:00:00Z","lastModified":"2026-07-17T01:00:00Z","location":"https://example.com/scim/v2/Users/ignored"}""",
        ),
      "groups" to
        objectMapper.readTree(
          """[{"value":"ignored","${'$'}ref":"https://example.com/scim/v2/Groups/ignored","display":"Ignored group"}]""",
        ),
      "password" to objectMapper.readTree("\"discarded\""),
    ).forEach { (path, value) ->
      val original = user()
      val result =
        ScimPatchProcessor
          .applyUser(
            original,
            patch("""[{"op":"replace","path":"$path","value":$value}]"""),
          ).resource

      assertThat(result).describedAs(path).isEqualTo(original)
    }

    listOf("id.value", "meta.unknown", "groups.value", "password.foo").forEach { path ->
      assertScimError("invalidPath") {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","path":"$path","value":"ignored"}]"""),
        )
      }
    }
  }

  @Test
  fun `User PATCH validates ignored attribute shapes for direct and pathless add and replace`() {
    val invalidOperations =
      listOf(
        """{"op":"add","path":"id","value":{}}""",
        """{"op":"replace","path":"id","value":[]}""",
        """{"op":"add","path":"id","value":1}""",
        """{"op":"replace","path":"id","value":true}""",
        """{"op":"add","path":"id","value":null}""",
        """{"op":"replace","path":"meta","value":[]}""",
        """{"op":"add","path":"meta","value":{"unknown":"value"}}""",
        """{"op":"replace","path":"meta","value":{"location":false}}""",
        """{"op":"add","path":"groups","value":1}""",
        """{"op":"replace","path":"groups","value":[{}]}""",
        """{"op":"add","path":"groups","value":{"value":"group-1","display":false}}""",
        """{"op":"replace","path":"password","value":{}}""",
        """{"op":"add","path":"password","value":null}""",
        """{"op":"add","value":{"id":false}}""",
        """{"op":"replace","value":{"meta":null}}""",
        """{"op":"add","value":{"groups":[{"value":[]} ]}}""",
        """{"op":"replace","value":{"password":1}}""",
      )

    invalidOperations.forEach { operation ->
      val original = user()

      val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(original, patch("[$operation]")) }

      assertThat(exception.status).describedAs(operation).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).describedAs(operation).isEqualTo("invalidValue")
      assertThat(original).describedAs(operation).isEqualTo(user())
    }
  }

  @Test
  fun `User PATCH accepts one ignored group object and does not validate remove values`() {
    val original = user()
    val result =
      ScimPatchProcessor
        .applyUser(
          original,
          patch(
            """[{"op":"add","path":"groups","value":{"value":"group-1","${'$'}ref":"https://example.com/scim/v2/Groups/group-1","display":"Group 1"}},{"op":"remove","path":"id","value":false},{"op":"remove","path":"meta","value":[]},{"op":"remove","path":"groups","value":1},{"op":"remove","path":"password","value":{}}]""",
          ),
        ).resource

    assertThat(result).isEqualTo(original)
  }

  @Test
  fun `non-normative PATCH path whitespace and separators use exact SCIM errors`() {
    listOf(
      " displayName",
      "displayName ",
      "name .givenName",
      "name. givenName",
      "name\\t.givenName",
    ).forEach { path ->
      assertScimError("invalidPath") {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","path":"$path","value":"Updated"}]"""),
        )
      }
    }

    listOf(
      "emails [type eq \\\"work\\\"].value",
      "emails[ type eq \\\"work\\\"].value",
      "emails[type  eq \\\"work\\\"].value",
      "emails[type eq  \\\"work\\\"].value",
      "emails[type eq \\\"work\\\" ].value",
      "emails[type eq \\\"work\\\"] .value",
      "emails[type\\teq \\\"work\\\"].value",
    ).forEach { path ->
      assertScimError("invalidFilter") {
        ScimPatchProcessor.applyUser(
          user(),
          patch("""[{"op":"replace","path":"$path","value":"new@example.com"}]"""),
        )
      }
    }

    listOf(
      "members [value eq \\\"one\\\"]",
      "members[ value eq \\\"one\\\"]",
      "members[value  eq \\\"one\\\"]",
      "members[value eq  \\\"one\\\"]",
      "members[value eq \\\"one\\\" ]",
    ).forEach { path ->
      assertScimError("invalidFilter") {
        ScimPatchProcessor.applyGroup(
          group(),
          patch("""[{"op":"remove","path":"$path"}]"""),
          TENANT,
          acceptingValidator(),
        )
      }
    }
  }

  @Test
  fun `Group supports pathless fields plus Okta and Entra membership operations sequentially`() {
    val validated = mutableListOf<ValidationCall>()
    val validator =
      ScimMemberValidator { configurationId, organizationId, memberIds ->
        validated += ValidationCall(configurationId, organizationId, memberIds)
        true
      }
    val request =
      patch(
        """
        [
          {"op":"replace","value":{"displayName":"Platform","externalId":"group-ext"}},
          {"op":"Add","path":"members","value":[{"value":"three"}]},
          {"op":"Remove","path":"members[value eq \"one\"]"},
          {"op":"REMOVE","path":"members","value":[{"value":"two"}]}
        ]
        """.trimIndent(),
      )

    val result = ScimPatchProcessor.applyGroup(group(), request, TENANT, validator)

    assertThat(result["displayName"].asText()).isEqualTo("Platform")
    assertThat(result["externalId"].asText()).isEqualTo("group-ext")
    assertThat(result["members"].map { it["value"].asText() }).containsExactly("three")
    assertThat(validated).containsExactly(
      ValidationCall(TENANT.configurationId, TENANT.organizationId, setOf("three", "one", "two")),
    )
  }

  @Test
  fun `Group member add creates the optional collection when it is absent`() {
    val original = group().also { it.remove("members") }

    val result =
      ScimPatchProcessor.applyGroup(
        original,
        patch("""[{"op":"add","path":"members","value":[{"value":"one"}]}]"""),
        TENANT,
        acceptingValidator(),
      )

    assertThat(result["members"].map { it["value"].asText() }).containsExactly("one")
    assertThat(original.has("members")).isFalse()
  }

  @Test
  fun `Okta filtered member removal is a no-op when the optional collection is absent`() {
    val original = group().also { it.remove("members") }

    val result =
      ScimPatchProcessor.applyGroup(
        original,
        patch("""[{"op":"remove","path":"members[value eq \"one\"]"}]"""),
        TENANT,
        acceptingValidator(),
      )

    assertThat(result).isEqualTo(original)
  }

  @Test
  fun `Entra member value-array removal is a no-op when the optional collection is absent`() {
    val original = group().also { it.remove("members") }

    val result =
      ScimPatchProcessor.applyGroup(
        original,
        patch("""[{"op":"remove","path":"members","value":[{"value":"one"}]}]"""),
        TENANT,
        acceptingValidator(),
      )

    assertThat(result).isEqualTo(original)
  }

  @Test
  fun `Okta filtered member removal rejects every invalid submitted reference atomically`() {
    listOf("inactive", "unknown", "cross-configuration", "cross-organization").forEach { invalidMemberId ->
      val original = group().also { (it["members"][0] as ObjectNode).put("value", invalidMemberId) }
      val expected = original.deepCopy()
      val exception =
        assertThrows<ScimException> {
          ScimPatchProcessor.applyGroup(
            original,
            patch("""[{"op":"remove","path":"members[value eq \"$invalidMemberId\"]"}]"""),
            TENANT,
            ScimMemberValidator { configurationId, organizationId, memberIds ->
              assertThat(configurationId).isEqualTo(TENANT.configurationId)
              assertThat(organizationId).isEqualTo(TENANT.organizationId)
              assertThat(memberIds).containsExactlyInAnyOrder(invalidMemberId, "two")
              false
            },
          )
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(expected)
    }
  }

  @Test
  fun `Entra member value-array removal rejects every invalid submitted reference atomically`() {
    listOf("inactive", "unknown", "cross-configuration", "cross-organization").forEach { invalidMemberId ->
      val original = group().also { (it["members"][0] as ObjectNode).put("value", invalidMemberId) }
      val expected = original.deepCopy()
      val exception =
        assertThrows<ScimException> {
          ScimPatchProcessor.applyGroup(
            original,
            patch("""[{"op":"remove","path":"members","value":[{"value":"$invalidMemberId"}]}]"""),
            TENANT,
            ScimMemberValidator { configurationId, organizationId, memberIds ->
              assertThat(configurationId).isEqualTo(TENANT.configurationId)
              assertThat(organizationId).isEqualTo(TENANT.organizationId)
              assertThat(memberIds).containsExactlyInAnyOrder(invalidMemberId, "two")
              false
            },
          )
        }

      assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
      assertThat(exception.scimType).isEqualTo("invalidValue")
      assertThat(original).isEqualTo(expected)
    }
  }

  @Test
  fun `Group member removals validate an active User who is not currently a member`() {
    listOf(
      """[{"op":"remove","path":"members[value eq \"active-non-member\"]"}]""",
      """[{"op":"remove","path":"members","value":[{"value":"active-non-member"}]}]""",
    ).forEach { operations ->
      val validated = mutableListOf<ValidationCall>()
      val original = group().also { it.remove("members") }

      val result =
        ScimPatchProcessor.applyGroup(
          original,
          patch(operations),
          TENANT,
          ScimMemberValidator { configurationId, organizationId, memberIds ->
            validated += ValidationCall(configurationId, organizationId, memberIds)
            true
          },
        )

      assertThat(result).isEqualTo(original)
      assertThat(validated).containsExactly(
        ValidationCall(TENANT.configurationId, TENANT.organizationId, setOf("active-non-member")),
      )
    }
  }

  @Test
  fun `Entra member removal still requires a submitted value when the optional collection is absent`() {
    listOf(
      """[{"op":"remove","path":"members"}]""",
      """[{"op":"remove","path":"members","value":null}]""",
    ).forEach { operations ->
      assertScimError("invalidValue") {
        ScimPatchProcessor.applyGroup(
          group().also { it.remove("members") },
          patch(operations),
          TENANT,
          acceptingValidator(),
        )
      }
    }
  }

  @Test
  fun `Group replacement de-duplicates members and ignores read-only enrichment`() {
    val result =
      ScimPatchProcessor.applyGroup(
        group(),
        patch(
          """[{"op":"replace","path":"members","value":[{"value":"two","display":"ignored","${'$'}ref":"ignored"},{"value":"two"}]}]""",
        ),
        TENANT,
        acceptingValidator(),
      )

    assertThat(result["members"]).hasSize(1)
    assertThat(result["members"][0].fieldNames().asSequence().toList()).containsExactly("value")
  }

  @Test
  fun `pathless Group operation rejects membership changes`() {
    assertScimError("invalidPath") {
      ScimPatchProcessor.applyGroup(
        group(),
        patch("""[{"op":"add","value":{"members":[{"value":"three"}]}}]"""),
        TENANT,
        acceptingValidator(),
      )
    }
  }

  @Test
  fun `invalid cross-scope inactive or unknown member validation rolls back atomically`() {
    val original = group()
    var receivedTenant: ScimTenant? = null
    val validator =
      ScimMemberValidator { configurationId, organizationId, _ ->
        receivedTenant = ScimTenant(configurationId, organizationId)
        false
      }

    val exception =
      assertThrows<ScimException> {
        ScimPatchProcessor.applyGroup(
          original,
          patch("""[{"op":"add","path":"members","value":[{"value":"cross-tenant"}]}]"""),
          TENANT,
          validator,
        )
      }

    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo("invalidValue")
    assertThat(receivedTenant).isEqualTo(TENANT)
    assertThat(original).isEqualTo(group())
  }

  @Test
  fun `membership validation failures propagate without returning a partial result`() {
    val failure = IllegalStateException("repository unavailable")

    assertThatThrownBy {
      ScimPatchProcessor.applyGroup(
        group(),
        patch("""[{"op":"add","path":"members","value":[{"value":"three"}]}]"""),
        TENANT,
        ScimMemberValidator { _, _, _ -> throw failure },
      )
    }.isSameAs(failure)
  }

  private fun assertScimError(
    scimType: String,
    block: () -> Unit,
  ) {
    val exception = assertThrows<ScimException> { block() }
    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo(scimType)
  }

  private fun assertInvalidEmailPatchIsAtomic(operations: String) {
    val original = user()
    val exception = assertThrows<ScimException> { ScimPatchProcessor.applyUser(original, patch(operations)) }

    assertThat(exception.status).isEqualTo(HttpStatus.BAD_REQUEST)
    assertThat(exception.scimType).isEqualTo("invalidValue")
    assertThat(original).isEqualTo(user())
  }

  private fun acceptingValidator() = ScimMemberValidator { _, _, _ -> true }

  private fun patch(operations: String): ObjectNode = json("""{"schemas":["$SCIM_PATCH_OP_SCHEMA"],"Operations":$operations}""")

  private fun json(value: String): ObjectNode = objectMapper.readTree(value) as ObjectNode

  private fun user(): ObjectNode =
    json(
      """
      {
        "schemas":["$SCIM_USER_SCHEMA"],
        "id":"user-id",
        "externalId":"external",
        "userName":"alice@example.com",
        "active":true,
        "name":{"givenName":"Original"},
        "emails":[{"value":"alice@example.com","type":"work","primary":true}],
        "meta":{"resourceType":"User"}
      }
      """.trimIndent(),
    )

  private fun group(): ObjectNode =
    json(
      """
      {
        "schemas":["$SCIM_GROUP_SCHEMA"],
        "id":"group-id",
        "displayName":"Engineering",
        "members":[{"value":"one"},{"value":"two"}],
        "meta":{"resourceType":"Group"}
      }
      """.trimIndent(),
    )

  private data class ValidationCall(
    val configurationId: UUID,
    val organizationId: UUID,
    val memberIds: Set<String>,
  )

  companion object {
    private val TENANT =
      ScimTenant(
        configurationId = UUID.fromString("11111111-1111-1111-1111-111111111111"),
        organizationId = UUID.fromString("22222222-2222-2222-2222-222222222222"),
      )
  }
}
