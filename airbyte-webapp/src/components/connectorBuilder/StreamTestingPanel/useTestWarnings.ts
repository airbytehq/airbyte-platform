import { useMemo } from "react";
import { useIntl } from "react-intl";

import {
  useConnectorBuilderFormState,
  useConnectorBuilderTestRead,
} from "services/connectorBuilder/ConnectorBuilderStateService";

export const useTestWarnings = () => {
  const { formatMessage } = useIntl();
  const { editorView, builderFormValues } = useConnectorBuilderFormState();
  const {
    testStreamIndex,
    streamRead: { data: streamReadData },
  } = useConnectorBuilderTestRead();

  const currentStream = editorView === "ui" && builderFormValues.streams[testStreamIndex];

  return useMemo(() => {
    if (!currentStream || !streamReadData) {
      return [];
    }
    const warnings: string[] = [];
    if (currentStream.fieldPointer.length) {
      const fieldPointerMissing = streamReadData.slices.every((slice) => {
        return slice.pages.every((page) => {
          // check whether responses look like they contain data but are always empty
          return page.response?.body && page.response.status === 200 && page.records.length === 0;
        });
      });
      if (fieldPointerMissing) {
        warnings.push(formatMessage({ id: "connectorBuilder.warnings.noRecords" }));
      }
    }
    if (currentStream.primaryKey.length) {
      const allPrimaryKeysSet = streamReadData.slices.every((slice) => {
        return slice.pages.every((page) => {
          return page.records.every((record) => {
            // at least one of the key parts needs to be set
            return currentStream.primaryKey.some((keyPart) => typeof record[keyPart] !== "undefined");
          });
        });
      });
      if (!allPrimaryKeysSet) {
        warnings.push(formatMessage({ id: "connectorBuilder.warnings.primaryKeyMissing" }));
      }

      const primaryKeys = streamReadData.slices.flatMap((slice) => {
        return slice.pages.flatMap((page) => {
          return page.records.flatMap((record) => {
            return currentStream.primaryKey.map((keyPart) => JSON.stringify(record[keyPart]) ?? "-").join(", ");
          });
        });
      });

      const keySet = new Set();
      const duplicateKey = primaryKeys.find((key) => {
        if (keySet.has(key)) {
          return true;
        }
        keySet.add(key);
        return false;
      });
      if (duplicateKey) {
        warnings.push(formatMessage({ id: "connectorBuilder.warnings.primaryKeyDuplicate" }, { duplicateKey }));
      }
    }
    return warnings;
  }, [currentStream, formatMessage, streamReadData]);
};
