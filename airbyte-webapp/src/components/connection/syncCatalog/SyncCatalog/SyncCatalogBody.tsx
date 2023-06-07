import { Field, FieldProps, setIn } from "formik";
import React, { useCallback } from "react";

import { FormikConnectionFormValues } from "components/connection/ConnectionForm/formConfig";

import { SyncSchemaStream } from "core/domain/catalog";
import { AirbyteStreamConfiguration } from "core/request/AirbyteClient";

import styles from "./SyncCatalogBody.module.scss";
import { SyncCatalogEmpty } from "./SyncCatalogEmpty";
import { SyncCatalogRow } from "./SyncCatalogRow";
import { StreamsConfigTableHeader } from "../StreamsConfigTable";
import { StreamsConfigTableConnectorHeader } from "../StreamsConfigTable/StreamsConfigTableConnectorHeader";

interface SyncCatalogBodyProps {
  streams: SyncSchemaStream[];
  onStreamsChanged: (streams: SyncSchemaStream[]) => void;
  onStreamChanged: (stream: SyncSchemaStream) => void;
  isFilterApplied?: boolean;
}

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

  return (
    <div data-testid="catalog-tree-table-body">
      <div className={styles.header}>
        <StreamsConfigTableConnectorHeader />
        <StreamsConfigTableHeader
          streams={streams}
          onStreamsChanged={onStreamsChanged}
          syncSwitchDisabled={isFilterApplied}
        />
      </div>
      {streams.length ? (
        streams.map((streamNode) => (
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
        ))
      ) : (
        <SyncCatalogEmpty customText={isFilterApplied ? "connection.catalogTree.noMatchingStreams" : ""} />
      )}
    </div>
  );
};
