/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories.domain;

import io.airbyte.db.instance.configs.jooq.generated.enums.InvitationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType;
import io.micronaut.data.annotation.AutoPopulated;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.data.annotation.TypeDef;
import io.micronaut.data.model.DataType;
import java.time.OffsetDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Builder(toBuilder = true)
@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode
@MappedEntity("user_invitation")
public class UserInvitation {

  @Id
  @AutoPopulated
  private UUID id;

  private String inviteCode;

  private UUID inviterUserId;

  private String invitedEmail;

  private UUID scopeId;

  @TypeDef(type = DataType.OBJECT)
  private ScopeType scopeType;

  @TypeDef(type = DataType.OBJECT)
  private PermissionType permissionType;

  @TypeDef(type = DataType.OBJECT)
  private InvitationStatus status;

  @DateCreated
  private OffsetDateTime createdAt;

  @DateUpdated
  private OffsetDateTime updatedAt;

}
