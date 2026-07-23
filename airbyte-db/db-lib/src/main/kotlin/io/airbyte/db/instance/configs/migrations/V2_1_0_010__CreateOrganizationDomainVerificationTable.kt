/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_010__CreateOrganizationDomainVerificationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    createDomainVerificationStatusEnumType(ctx)
    createDomainVerificationMethodEnumType(ctx)
    createOrganizationDomainVerificationTableAndIndexes(ctx)
  }

  /**
   * new DomainVerificationStatus enum.
   */
  enum class DomainVerificationStatus(
    private val literal: String,
  ) : EnumType {
    PENDING("pending"),
    VERIFIED("verified"),
    FAILED("failed"),
    EXPIRED("expired"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "domain_verification_status"
    }
  }

  /**
   * new DomainVerificationMethod enum.
   * DNS_TXT → 'dns_txt' (standard DNS TXT record verification)
   * LEGACY → 'legacy' (grandfathered domains from existing SSO orgs, no verification needed)
   */
  enum class DomainVerificationMethod(
    private val literal: String,
  ) : EnumType {
    DNS_TXT("dns_txt"),
    LEGACY("legacy"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "domain_verification_method"
    }
  }

  companion object {
    private fun createDomainVerificationStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(DomainVerificationStatus.NAME)
        .asEnum(*DomainVerificationStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createDomainVerificationMethodEnumType(ctx: DSLContext) {
      ctx
        .createType(DomainVerificationMethod.NAME)
        .asEnum(*DomainVerificationMethod.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    // create organization_domain_verification table and all its indexes
    private fun createOrganizationDomainVerificationTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val organizationId = DSL.field("organization_id", SQLDataType.UUID.nullable(false))
      val domain = DSL.field("domain", SQLDataType.VARCHAR(255).nullable(false))
      val verificationToken = DSL.field("verification_token", SQLDataType.VARCHAR(255).nullable(true))
      val status =
        DSL.field(
          "status",
          SQLDataType.VARCHAR
            .asEnumDataType(DomainVerificationStatus::class.java)
            .nullable(false)
            .defaultValue(DomainVerificationStatus.PENDING),
        )
      val verificationMethod =
        DSL.field(
          "verification_method",
          SQLDataType.VARCHAR
            .asEnumDataType(DomainVerificationMethod::class.java)
            .nullable(false)
            .defaultValue(DomainVerificationMethod.DNS_TXT),
        )
      val dnsRecordName = DSL.field("dns_record_name", SQLDataType.VARCHAR(512).nullable(true))
      val dnsRecordPrefix = DSL.field("dns_record_prefix", SQLDataType.VARCHAR(512).nullable(true))
      val attempts = DSL.field("attempts", SQLDataType.INTEGER.nullable(false).defaultValue(0))
      val lastCheckedAt = DSL.field("last_checked_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      val expiresAt = DSL.field("expires_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      val createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(true))
      val verifiedAt = DSL.field("verified_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      // create the table
      ctx
        .createTableIfNotExists("organization_domain_verification")
        .columns(
          id,
          organizationId,
          domain,
          verificationToken,
          status,
          verificationMethod,
          dnsRecordName,
          dnsRecordPrefix,
          attempts,
          lastCheckedAt,
          expiresAt,
          createdBy,
          verifiedAt,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.unique(organizationId, domain),
          DSL
            .foreignKey<UUID>(organizationId)
            .references(ORGANIZATION_TABLE, "id")
            .onDeleteCascade(),
          DSL
            .foreignKey<UUID>(createdBy)
            .references(USER_TABLE, "id")
            .onDeleteSetNull(),
        ).execute()

      // add the conditional CHECK constraint
      addDnsFieldsRequiredConstraint(ctx)

      // create an index on domain_verification_status
      ctx
        .createIndexIfNotExists("idx_domain_verification_status")
        .on("organization_domain_verification", "status")
        .execute()

      // create an index on domain_verification_org
      ctx
        .createIndexIfNotExists("idx_domain_verification_org")
        .on("organization_domain_verification", "organization_id")
        .execute()

      // create an index on domain column
      ctx
        .createIndexIfNotExists("idx_domain_verification_domain")
        .on("organization_domain_verification", "domain")
        .execute()

      // create an index on created_by column
      ctx
        .createIndexIfNotExists("idx_domain_verification_created_by")
        .on("organization_domain_verification", "created_by")
        .execute()
    }

    private fun addDnsFieldsRequiredConstraint(ctx: DSLContext) {
      val constraintSql =
        """
        verification_method = 'legacy'
        OR (
          verification_token IS NOT NULL AND verification_token != '' AND
          dns_record_name IS NOT NULL AND dns_record_name != '' AND
          dns_record_prefix IS NOT NULL AND dns_record_prefix != ''
        )
        """.trimIndent()

      ctx
        .alterTable("organization_domain_verification")
        .add(DSL.constraint("check_dns_fields_required_for_dns_txt").check(DSL.condition(constraintSql)))
        .execute()
    }
  }
}
