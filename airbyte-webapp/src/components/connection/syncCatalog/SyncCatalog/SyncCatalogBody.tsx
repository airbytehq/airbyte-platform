import { Field, FieldProps, setIn } from "formik";
import React, { useCallback, useMemo } from "react";
import { Location, useLocation } from "react-router-dom";
import { IndexLocationWithAlign, Virtuoso } from "react-virtuoso";

import { FormikConnectionFormValues } from "components/connection/ConnectionForm/formConfig";

import { SyncSchemaStream } from "core/domain/catalog";
import { AirbyteStreamConfiguration } from "core/request/AirbyteClient";

import styles from "./SyncCatalogBody.module.scss";
import { SyncCatalogEmpty } from "./SyncCatalogEmpty";
import { SyncCatalogRow } from "./SyncCatalogRow";
import { StreamsConfigTableHeader } from "../StreamsConfigTable";

interface RedirectionLocationState {
  namespace?: string;
  streamName?: string;
  action?: "showInReplicationTable" | "openDetails";
}

export interface LocationWithState extends Location {
  state: RedirectionLocationState;
}

interface SyncCatalogBodyProps {
  streams: SyncSchemaStream[];
  onStreamsChanged: (streams: SyncSchemaStream[]) => void;
  onStreamChanged: (stream: SyncSchemaStream) => void;
  isFilterApplied?: boolean;
}
/**
 * @deprecated will be removed during clean up - https://github.com/airbytehq/airbyte-platform-internal/issues/8639
 * use SyncCatalogHookFormField.tsx instead
 * @see SyncCatalogHookFormField
 */
export const SyncCatalogBody: React.FC<SyncCatalogBodyProps> = ({
  streams,
  onStreamsChanged,
  onStreamChanged,
  isFilterApplied = false,
}) => {
  const onUpdateStream = useCallback(
    (id: string | undefined, newConfig: Partial<AirbyteStreamConfiguration>) => {
      const streamNode = streams.find((streamNode) => streamNode.id === id);

      if (streamNode) {
        const newStreamNode = setIn(streamNode, "config", { ...streamNode.config, ...newConfig });

        // config.selectedFields must be removed if fieldSelection is disabled
        if (!newStreamNode.config.fieldSelectionEnabled) {
          delete newStreamNode.config.selectedFields;
        }

        onStreamChanged(newStreamNode);
      }
    },
    [streams, onStreamChanged]
  );

  // Scroll to the stream that was redirected from the Status tab
  const { state: locationState } = useLocation() as LocationWithState;
  const initialTopMostItemIndex: IndexLocationWithAlign | undefined = useMemo(() => {
    if (locationState?.action !== "showInReplicationTable" && locationState?.action !== "openDetails") {
      return;
    }

    return {
      index: streams.findIndex(
        (stream) =>
          stream.stream?.name === locationState?.streamName && stream.stream?.namespace === locationState?.namespace
      ),
      align: "center",
    };
  }, [locationState?.action, locationState?.namespace, locationState?.streamName, streams]);

  return (
    <div data-testid="catalog-tree-table-body">
      <div className={styles.header}>
        <StreamsConfigTableHeader
          streams={streams}
          onStreamsChanged={onStreamsChanged}
          syncSwitchDisabled={isFilterApplied}
        />
      </div>
      {streams.length ? (
        <Virtuoso
          style={{ height: "40vh" }}
          data={streams}
          initialTopMostItemIndex={initialTopMostItemIndex}
          fixedItemHeight={50}
          itemContent={(_index, streamNode) => (
            <Field key={`schema.streams[${streamNode.id}].config`} name={`schema.streams[${streamNode.id}].config`}>
              {({ form }: FieldProps<FormikConnectionFormValues>) => (
                <SyncCatalogRow
                  key={`schema.streams[${streamNode.id}].config`}
                  errors={form.errors}
                  namespaceDefinition={form.values.namespaceDefinition}
                  namespaceFormat={form.values.namespaceFormat}
                  prefix={form.values.prefix}
                  streamNode={streamNode}
                  updateStream={onUpdateStream}
                />
              )}
            </Field>
          )}
        />
      ) : (
        <SyncCatalogEmpty customText={isFilterApplied ? "connection.catalogTree.noMatchingStreams" : ""} />
      )}
    </div>
  );
};
