import classNames from "classnames";
import { diffJson, Change } from "diff";
import isEqual from "lodash/isEqual";
import merge from "lodash/merge";
import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useDebounce } from "react-use";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Pre } from "components/ui/Pre";
import { Tooltip } from "components/ui/Tooltip";

import { StreamReadInferredSchema } from "core/api/types/ConnectorBuilderClient";
import { DeclarativeStreamSchemaLoader, InlineSchemaLoaderType } from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";

import styles from "./SchemaDiffView.module.scss";
import { SchemaConflictMessage } from "../SchemaConflictMessage";
import { useBuilderWatch } from "../useBuilderWatch";
import { useStreamName } from "../useStreamNames";
import { getStreamFieldPath, formatJson } from "../utils";

interface SchemaDiffViewProps {
  inferredSchema: StreamReadInferredSchema;
  incompatibleErrors?: string[];
}

interface Diff {
  /**
   * List of changes from current schema to detected schema
   */
  changes: Change[];
  /**
   * Formatted merged schema if merging in the detected schema changes the existing schema
   */
  mergedSchema?: object;
  /**
   * Flag if overriding the existing schema with the new schema would lose information
   */
  lossyOverride: boolean;
}

function getDiff(existingSchema: object, detectedSchema: object): Diff {
  try {
    const mergedSchemaPreferExisting = merge({}, detectedSchema, existingSchema);
    const changes = diffJson(existingSchema, detectedSchema);
    // The override would be lossy if lines are removed in the diff
    const lossyOverride = changes.some((change) => change.removed);
    return {
      changes,
      mergedSchema: !isEqual(mergedSchemaPreferExisting, existingSchema) ? mergedSchemaPreferExisting : undefined,
      lossyOverride,
    };
  } catch {
    return { changes: [], lossyOverride: true };
  }
}

export const SchemaDiffView: React.FC<SchemaDiffViewProps> = ({ inferredSchema, incompatibleErrors }) => {
  const analyticsService = useAnalyticsService();
  const mode = useBuilderWatch("mode");
  const testStreamId = useBuilderWatch("testStreamId");
  const streamName = useStreamName(testStreamId);
  const { setValue } = useFormContext();
  const schemaLoaderPath = useMemo(() => getStreamFieldPath(testStreamId, "schema_loader", true), [testStreamId]);
  const declaredSchemaLoader = useBuilderWatch(schemaLoaderPath) as DeclarativeStreamSchemaLoader | undefined;
  const declaredSchema = useMemo(() => {
    if (!declaredSchemaLoader) {
      return undefined;
    }
    if (Array.isArray(declaredSchemaLoader)) {
      return undefined;
    }
    if (declaredSchemaLoader.type !== InlineSchemaLoaderType.InlineSchemaLoader) {
      return undefined;
    }
    return declaredSchemaLoader.schema;
  }, [declaredSchemaLoader]);
  const declaredAndInferredSchemasAreEqual = useMemo(
    () => isEqual(declaredSchema, inferredSchema),
    [declaredSchema, inferredSchema]
  );

  const [schemaDiff, setSchemaDiff] = useState<Diff>(() =>
    mode === "ui" && declaredSchema ? getDiff(declaredSchema, inferredSchema) : { changes: [], lossyOverride: false }
  );

  useDebounce(
    () => {
      if (mode === "ui" && declaredSchema) {
        setSchemaDiff(getDiff(declaredSchema, inferredSchema));
      }
    },
    250,
    [declaredSchemaLoader, inferredSchema, mode]
  );

  return (
    <FlexContainer direction="column">
      {mode === "ui" &&
        testStreamId.type !== "generated_stream" &&
        declaredSchema &&
        !declaredAndInferredSchemasAreEqual && (
          <Message type="warning" text={<SchemaConflictMessage errors={incompatibleErrors} />}>
            <FlexItem grow className={styles.mergeButtons}>
              <FlexContainer direction="column">
                <FlexContainer>
                  <FlexItem grow>
                    <Button
                      full
                      type="button"
                      variant="primaryDark"
                      disabled={declaredAndInferredSchemasAreEqual}
                      onClick={() => {
                        setValue(schemaLoaderPath, {
                          type: InlineSchemaLoaderType.InlineSchemaLoader,
                          schema: inferredSchema,
                        });
                        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
                          actionDescription: "Declared schema overwritten by detected schema",
                          stream_name: streamName,
                        });
                      }}
                    >
                      <FormattedMessage
                        id={
                          schemaDiff.lossyOverride
                            ? "connectorBuilder.overwriteSchemaButton"
                            : "connectorBuilder.useSchemaButton"
                        }
                      />
                    </Button>
                  </FlexItem>
                  {schemaDiff.mergedSchema && schemaDiff.lossyOverride && (
                    <FlexItem grow>
                      <Tooltip
                        control={
                          <Button
                            full
                            variant="primaryDark"
                            type="button"
                            onClick={() => {
                              setValue(schemaLoaderPath, {
                                type: InlineSchemaLoaderType.InlineSchemaLoader,
                                schema: schemaDiff.mergedSchema,
                              });
                              analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.MERGE_SCHEMA, {
                                actionDescription: "Detected and Declared schemas merged to update declared schema",
                                stream_name: streamName,
                              });
                            }}
                          >
                            <FormattedMessage id="connectorBuilder.mergeSchemaButton" />
                          </Button>
                        }
                      >
                        <FormattedMessage id="connectorBuilder.mergeSchemaTooltip" />
                      </Tooltip>
                    </FlexItem>
                  )}
                </FlexContainer>
              </FlexContainer>
            </FlexItem>
          </Message>
        )}
      {mode === "ui" && !declaredSchema && (
        <Button
          full
          variant="secondary"
          type="button"
          onClick={() => {
            setValue(schemaLoaderPath, {
              type: InlineSchemaLoaderType.InlineSchemaLoader,
              schema: inferredSchema,
            });
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
              actionDescription: "Declared schema overwritten by detected schema",
              stream_name: streamName,
            });
          }}
          data-testid="accept-schema"
        >
          <FormattedMessage id="connectorBuilder.useSchemaButton" />
        </Button>
      )}
      <FlexItem>
        {mode === "yaml" || !schemaDiff.changes.length || !declaredSchema ? (
          <Pre className={styles.diffLine}>
            {formatJson(inferredSchema, true)
              .split("\n")
              .map((line) => ` ${line}`)
              .join("\n")}
          </Pre>
        ) : (
          schemaDiff.changes.map((change, changeIndex) => (
            <Pre
              className={classNames(
                {
                  [styles.added]: change.added,
                  [styles.removed]: change.removed,
                },
                styles.diffLine
              )}
              key={changeIndex}
            >
              {change.value
                .split("\n")
                .map((line) => (line === "" ? undefined : `${change.added ? "+" : change.removed ? "-" : " "}${line}`))
                .filter(Boolean)
                .join("\n")}
            </Pre>
          ))
        )}
      </FlexItem>
    </FlexContainer>
  );
};
