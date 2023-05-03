/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.repositories.domain;

import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusIncompleteRunCause;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusJobType;
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStreamStatusRunState;
import io.hypersistence.utils.hibernate.type.basic.PostgreSQLEnumType;
import io.micronaut.data.annotation.DateCreated;
import io.micronaut.data.annotation.DateUpdated;
import java.time.OffsetDateTime;
import java.util.UUID;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

/**
 * DTO for our data access layer.
 */
@Builder
@AllArgsConstructor // The builder uses this constructor
@NoArgsConstructor // Hibernate uses this constructor and sets the values individually
@Getter
@Setter
@EqualsAndHashCode(exclude = {"id", "createdAt", "updatedAt"})
@Entity(name = "stream_statuses")
@TypeDef(name = StreamStatus.PGSQL_ENUM,
         typeClass = PostgreSQLEnumType.class)
public class StreamStatus {

  static final String PGSQL_ENUM = "pgsql_enum";

  @Id
  @GeneratedValue
  private UUID id;

  private UUID workspaceId;

  private UUID connectionId;

  private Long jobId;

  private Integer attemptNumber;

  private String streamNamespace;

  private String streamName;

  @Enumerated(EnumType.STRING)
  @Type(type = PGSQL_ENUM)
  private JobStreamStatusJobType jobType;

  @DateCreated
  private OffsetDateTime createdAt;

  @DateUpdated
  private OffsetDateTime updatedAt;

  @Enumerated(EnumType.STRING)
  @Type(type = PGSQL_ENUM)
  private JobStreamStatusRunState runState;

  @Enumerated(EnumType.STRING)
  @Type(type = PGSQL_ENUM)
  private JobStreamStatusIncompleteRunCause incompleteRunCause;

  private OffsetDateTime transitionedAt;

}
