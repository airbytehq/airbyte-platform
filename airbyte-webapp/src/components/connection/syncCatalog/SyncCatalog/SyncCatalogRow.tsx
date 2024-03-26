import get from "lodash/get";
import set from "lodash/set";
import React, { useCallback, useMemo } from "react";
import { FieldErrors } from "react-hook-form";
import { useToggle } from "react-use";

import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { traverseSchemaToField } from "core/domain/catalog/traverseSchemaToField";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { naturalComparatorBy } from "core/utils/objects";
import { useDestinationNamespace } from "hooks/connection/useDestinationNamespace";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

import { updateStreamSyncMode } from "./updateStreamSyncMode";
import { FormConnectionFormValues, SyncStreamFieldWithId, SUPPORTED_MODES } from "../../ConnectionForm/formConfig";
import { StreamDetailsPanel } from "../StreamDetailsPanel/StreamDetailsPanel";
import { StreamsConfigTableRow } from "../StreamsConfigTable/StreamsConfigTableRow";
import { SyncModeValue } from "../SyncModeSelect";
import { checkCursorAndPKRequirements, getFieldPathType } from "../utils";

interface SyncCatalogRowProps
  extends Pick<FormConnectionFormValues, "namespaceDefinition" | "namespaceFormat" | "prefix"> {
  streamNode: SyncStreamFieldWithId;
  updateStreamNode: (newStreamNode: SyncStreamFieldWithId) => void;
  errors: FieldErrors<FormConnectionFormValues>;
}

/**
 * react-hook-form sync catalog row component
 */
export const SyncCatalogRow: React.FC<SyncCatalogRowProps & { className?: string }> = ({
  streamNode,
  updateStreamNode,
  namespaceDefinition,
  namespaceFormat,
  prefix,
  errors,
  className,
}) => {
  const { stream, config } = streamNode;

  const fields = useMemo(() => {
    const traversedFields = traverseSchemaToField(stream?.jsonSchema, stream?.name);
    return traversedFields.sort(naturalComparatorBy((field) => field.cleanedName));
  }, [stream?.jsonSchema, stream?.name]);

  const {
    destDefinitionSpecification: { supportedDestinationSyncModes },
  } = useConnectionFormService();
  const { mode } = useConnectionFormService();

  const [isStreamDetailsPanelOpened, setIsStreamDetailsPanelOpened] = useToggle(false);

  const updateStreamWithConfig = useCallback(
    (configObj: Partial<AirbyteStreamConfiguration>) => {
      const updatedStreamNode = set(streamNode, "config", {
        ...streamNode.config,
        ...configObj,
      });

      // config.selectedFields must be removed if fieldSelection is disabled
      if (!updatedStreamNode.config?.fieldSelectionEnabled) {
        delete updatedStreamNode.config?.selectedFields;
      }

      updateStreamNode(updatedStreamNode);
    },
    [streamNode, updateStreamNode]
  );

  const isSimplifiedCreation = useExperiment("connection.simplifiedCreation", false);
  const analyticsService = useAnalyticsService();
  const onSelectSyncMode = useCallback(
    (syncMode: SyncModeValue) => {
      if (!streamNode.config || !streamNode.stream) {
        return;
      }
      const updatedConfig = updateStreamSyncMode(streamNode.stream, streamNode.config, syncMode);
      updateStreamWithConfig(updatedConfig);

      if (isSimplifiedCreation) {
        analyticsService.track(Namespace.STREAM_SELECTION, Action.SET_SYNC_MODE, {
          actionDescription: "User selected a sync mode for a stream",
          streamNamespace: streamNode.stream.namespace,
          streamName: streamNode.stream.name,
          syncMode: syncMode.syncMode,
          destinationSyncMode: syncMode.destinationSyncMode,
        });
      }
    },
    [streamNode, updateStreamWithConfig, isSimplifiedCreation, analyticsService]
  );

  const onSelectStream = useCallback(
    () =>
      updateStreamWithConfig({
        selected: !(config && config.selected),
      }),
    [config, updateStreamWithConfig]
  );

  const { pkRequired, cursorRequired, shouldDefinePk, shouldDefineCursor } = checkCursorAndPKRequirements(
    config!,
    stream!
  );

  const availableSyncModes: SyncModeValue[] = useMemo(
    () =>
      SUPPORTED_MODES.filter(
        ([syncMode, destinationSyncMode]) =>
          stream?.supportedSyncModes?.includes(syncMode) && supportedDestinationSyncModes?.includes(destinationSyncMode)
      ).map(([syncMode, destinationSyncMode]) => ({
        syncMode,
        destinationSyncMode,
      })),
    [stream?.supportedSyncModes, supportedDestinationSyncModes]
  );

  const destNamespace =
    useDestinationNamespace(
      {
        namespaceDefinition,
        namespaceFormat,
      },
      stream?.namespace
    ) ?? "";

  const destName = `${prefix ? prefix : ""}${streamNode.stream?.name ?? ""}`;
  const configErrors = get(errors, `syncCatalog.streams[${stream?.name}_${stream?.namespace}].config`);
  const pkType = getFieldPathType(pkRequired, shouldDefinePk);
  const cursorType = getFieldPathType(cursorRequired, shouldDefineCursor);
  const hasFields = fields?.length > 0;
  const disabled = mode === "readonly";

  return (
    <>
      <StreamsConfigTableRow
        stream={streamNode}
        destNamespace={destNamespace}
        destName={destName}
        availableSyncModes={availableSyncModes}
        onSelectStream={onSelectStream}
        onSelectSyncMode={onSelectSyncMode}
        pkType={pkType}
        cursorType={cursorType}
        fields={fields}
        openStreamDetailsPanel={setIsStreamDetailsPanelOpened}
        configErrors={configErrors}
        disabled={disabled}
        className={className}
      />
      {isStreamDetailsPanelOpened && hasFields && stream && config && (
        <StreamDetailsPanel
          stream={stream}
          config={config}
          disabled={mode === "readonly"}
          fields={fields}
          onClose={setIsStreamDetailsPanelOpened}
          availableSyncModes={availableSyncModes}
          updateStreamWithConfig={updateStreamWithConfig}
        />
      )}
    </>
  );
};
