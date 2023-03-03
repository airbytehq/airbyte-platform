/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;

import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;

/**
 * This class centralises metrics queries. These queries power metrics that require some sort of
 * data access or calculation.
 * <p>
 * Simple metrics that require no calculation need not be tracked here.
 */
@Slf4j
public class MetricQueries {

  /**
   * Get releases stages for a job. Multiple release stages possible since source and destination can
   * have different release stages.
   *
   * @param ctx db context
   * @param jobId job id
   * @return releases stages for job
   */
  public static List<ReleaseStage> jobIdToReleaseStages(final DSLContext ctx, final long jobId) {
    final var srcRelStageCol = "src_release_stage";
    final var dstRelStageCol = "dst_release_stage";

    final var query = String.format("""
                                    SELECT src_def_data.release_stage AS %s,
                                           dest_def_data.release_stage AS %s
                                    FROM connection
                                    INNER JOIN jobs ON connection.id=CAST(jobs.scope AS uuid)
                                    INNER JOIN actor AS dest_data ON connection.destination_id = dest_data.id
                                    INNER JOIN actor_definition AS dest_def_data ON dest_data.actor_definition_id = dest_def_data.id
                                    INNER JOIN actor AS src_data ON connection.source_id = src_data.id
                                    INNER JOIN actor_definition AS src_def_data ON src_data.actor_definition_id = src_def_data.id
                                        WHERE jobs.id = '%d';""", srcRelStageCol, dstRelStageCol, jobId);

    final var res = ctx.fetch(query);
    final List<?> stages1 = res.getValues(srcRelStageCol);
    final List<?> stages2 = res.getValues(dstRelStageCol);
    return Stream.concat(stages1.stream(), stages2.stream())
        .filter(s -> s != null)
        .map(s -> ReleaseStage.valueOf(s.toString()))
        .collect(Collectors.toList());
  }

  /**
   * Get release stages for source and destination.
   *
   * @param ctx db context
   * @param srcId source id
   * @param dstId destination id
   * @return list of release stages
   */
  public static List<ReleaseStage> srcIdAndDestIdToReleaseStages(final DSLContext ctx, final UUID srcId, final UUID dstId) {
    return ctx.select(ACTOR_DEFINITION.RELEASE_STAGE).from(ACTOR).join(ACTOR_DEFINITION).on(ACTOR.ACTOR_DEFINITION_ID.eq(ACTOR_DEFINITION.ID))
        .where(ACTOR.ID.eq(srcId))
        .or(ACTOR.ID.eq(dstId)).fetch().getValues(ACTOR_DEFINITION.RELEASE_STAGE);
  }

}
