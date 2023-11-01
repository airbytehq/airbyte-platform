import classNames from "classnames";
import React, { useCallback, useMemo, useState } from "react";
import { useToggle } from "react-use";

import { LoadingBackdrop } from "components/ui/LoadingBackdrop";

import { SyncSchemaStream } from "core/domain/catalog";
import { naturalComparatorBy } from "core/utils/objects";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import { DisabledStreamsSwitch } from "./DisabledStreamsSwitch";
import styles from "./SyncCatalog.module.scss";
import { SyncCatalogBody } from "./SyncCatalogBody";
import { SyncCatalogStreamSearch } from "./SyncCatalogStreamSearch";
import { useStreamFilters } from "./useStreamFilters";

interface SyncCatalogProps {
  streams: SyncSchemaStream[];
  onStreamsChanged: (streams: SyncSchemaStream[]) => void;
  isLoading: boolean;
}
/**
 * @deprecated will be removed during clean up - https://github.com/airbytehq/airbyte-platform-internal/issues/8639
 * use SyncCatalogHookFormField.tsx instead
 * @see SyncCatalogHookFormField
 */
const SyncCatalogInternal: React.FC<React.PropsWithChildren<SyncCatalogProps>> = ({
  streams,
  onStreamsChanged,
  isLoading,
}) => {
  const { mode } = useConnectionFormService();

  const [searchString, setSearchString] = useState("");
  const [hideDisabledStreams, toggleHideDisabledStreams] = useToggle(false);

  const onSingleStreamChanged = useCallback(
    (newValue: SyncSchemaStream) => onStreamsChanged(streams.map((str) => (str.id === newValue.id ? newValue : str))),
    [streams, onStreamsChanged]
  );

  const sortedSchema = useMemo(
    () => [...streams].sort(naturalComparatorBy((syncStream) => syncStream.stream?.name ?? "")),
    [streams]
  );

  const filteredStreams = useStreamFilters(searchString, hideDisabledStreams, sortedSchema);

  return (
    <LoadingBackdrop loading={isLoading}>
      <SyncCatalogStreamSearch onSearch={setSearchString} />
      <DisabledStreamsSwitch checked={hideDisabledStreams} onChange={toggleHideDisabledStreams} />
      <div
        className={classNames(styles.bodyContainer, {
          [styles.scrollable]: mode === "create",
        })}
      >
        <SyncCatalogBody
          streams={filteredStreams}
          onStreamsChanged={onStreamsChanged}
          onStreamChanged={onSingleStreamChanged}
          isFilterApplied={filteredStreams.length !== streams.length}
        />
      </div>
    </LoadingBackdrop>
  );
};

export const SyncCatalog = React.memo(SyncCatalogInternal);
