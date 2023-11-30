import classNames from "classnames";
import { diffJson, Change } from "diff";
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
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderTestRead } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./SchemaDiffView.module.scss";
import { SchemaConflictMessage } from "../SchemaConflictMessage";
import { isEmptyOrDefault, useBuilderWatch } from "../types";
import { formatJson } from "../utils";

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
  mergedSchema?: string;
  /**
   * Flag if overriding the existing schema with the new schema would lose information
   */
  lossyOverride: boolean;
}

function getDiff(existingSchema: string | undefined, detectedSchema: object): Diff {
  if (!existingSchema) {
    return { changes: [], lossyOverride: false };
  }
  try {
    const existingObject = existingSchema ? JSON.parse(existingSchema) : undefined;
    const mergedSchemaPreferExisting = formatJson(merge({}, detectedSchema, existingObject), true);
    const changes = diffJson(existingObject, detectedSchema);
    // The override would be lossy if lines are removed in the diff
    const lossyOverride = changes.some((change) => change.removed);
    return {
      changes,
      mergedSchema: mergedSchemaPreferExisting !== existingSchema ? mergedSchemaPreferExisting : undefined,
      lossyOverride,
    };
  } catch {
    return { changes: [], lossyOverride: true };
  }
}

export const SchemaDiffView: React.FC<SchemaDiffViewProps> = ({ inferredSchema, incompatibleErrors }) => {
  const analyticsService = useAnalyticsService();
  const {
    resolvedManifest: { streams },
  } = useConnectorBuilderTestRead();
  const mode = useBuilderWatch("mode");
  const testStreamIndex = useBuilderWatch("testStreamIndex");
  const { setValue } = useFormContext();
  const path = `formValues.streams.${testStreamIndex}.schema` as const;
  const value = useBuilderWatch(path);
  const formattedSchema = useMemo(() => inferredSchema && formatJson(inferredSchema, true), [inferredSchema]);

  const [schemaDiff, setSchemaDiff] = useState<Diff>(() =>
    mode === "ui" ? getDiff(value, inferredSchema) : { changes: [], lossyOverride: false }
  );

  useDebounce(
    () => {
      if (mode === "ui") {
        setSchemaDiff(getDiff(value, inferredSchema));
      }
    },
    250,
    [value, inferredSchema, mode]
  );

  return (
    <FlexContainer direction="column">
      {mode === "ui" && !isEmptyOrDefault(value) && value !== formattedSchema && (
        <Message type="warning" text={<SchemaConflictMessage errors={incompatibleErrors} />}>
          <FlexItem grow className={styles.mergeButtons}>
            <FlexContainer direction="column">
              <FlexContainer>
                <FlexItem grow>
                  <Button
                    full
                    type="button"
                    variant="dark"
                    disabled={value === formattedSchema}
                    onClick={() => {
                      setValue(path, formattedSchema);
                      analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
                        actionDescription: "Declared schema overwritten by detected schema",
                        stream_name: streams[testStreamIndex]?.name,
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
                          variant="dark"
                          type="button"
                          onClick={() => {
                            setValue(path, schemaDiff.mergedSchema);
                            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.MERGE_SCHEMA, {
                              actionDescription: "Detected and Declared schemas merged to update declared schema",
                              stream_name: streams[testStreamIndex]?.name,
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
      {mode === "ui" && isEmptyOrDefault(value) && (
        <Button
          full
          variant="secondary"
          type="button"
          onClick={() => {
            setValue(path, formattedSchema);
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.OVERWRITE_SCHEMA, {
              actionDescription: "Declared schema overwritten by detected schema",
              stream_name: streams[testStreamIndex]?.name,
            });
          }}
          data-testid="accept-schema"
        >
          <FormattedMessage id="connectorBuilder.useSchemaButton" />
        </Button>
      )}
      <FlexItem>
        {mode === "yaml" || !schemaDiff.changes.length || isEmptyOrDefault(value) ? (
          <Pre className={styles.diffLine}>
            {formattedSchema
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
