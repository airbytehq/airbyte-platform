/* eslint-disable css-modules/no-unused-class */
import classNames from "classnames";
import { useMemo } from "react";

import { PillButtonVariant } from "components/ui/PillListBox/PillButton";

import { AirbyteStreamAndConfiguration, AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./StreamsConfigTableRow.module.scss";
import { isSameSyncStream } from "../../ConnectionForm/utils";
import { compareObjectsByFields } from "../utils";

export type StatusToDisplay = "disabled" | "added" | "removed" | "changed" | "unchanged";

export const useStreamsConfigTableRowProps = (stream: AirbyteStreamAndConfiguration) => {
  const { initialValues } = useConnectionFormService();

  const isStreamEnabled = stream.config?.selected;

  const statusToDisplay = useMemo<StatusToDisplay>(() => {
    const rowStatusChanged =
      initialValues.syncCatalog.streams.find(
        (item) => item.stream?.name === stream.stream?.name && item.stream?.namespace === stream.stream?.namespace
      )?.config?.selected !== stream.config?.selected;

    const rowChanged = !compareObjectsByFields<AirbyteStreamConfiguration>(
      initialValues.syncCatalog.streams.find((item) =>
        isSameSyncStream(item, stream.stream?.name, stream.stream?.namespace)
      )?.config,
      stream.config,
      ["selected", "cursorField", "destinationSyncMode", "primaryKey", "selectedFields", "syncMode", "aliasName"]
    );

    if (!isStreamEnabled && !rowStatusChanged) {
      return "disabled";
    } else if (rowStatusChanged) {
      return isStreamEnabled ? "added" : "removed";
    } else if (rowChanged) {
      return "changed";
    }
    return "unchanged";
  }, [initialValues.syncCatalog.streams, isStreamEnabled, stream.config, stream.stream]);

  const pillButtonVariant = useMemo<PillButtonVariant>(() => {
    if (statusToDisplay === "added") {
      return "green";
    } else if (statusToDisplay === "removed") {
      return "red";
    } else if (statusToDisplay === "changed") {
      return "blue";
    }
    return "grey";
  }, [statusToDisplay]);

  const streamHeaderContentStyle = classNames(styles.streamHeaderContent, {
    [styles.added]: statusToDisplay === "added",
    [styles.removed]: statusToDisplay === "removed",
    [styles.changed]: statusToDisplay === "changed",
    [styles.disabled]: statusToDisplay === "disabled",
  });

  return {
    streamHeaderContentStyle,
    statusToDisplay,
    pillButtonVariant,
  };
};
