import React from "react";
import { FormattedMessage } from "react-intl";

import { StreamsListHeaderListbox } from "./StreamsListHeaderListbox";

export const SyncMetricOption = {
  bytes: "bytes",
  records: "records",
} as const;

// eslint-disable-next-line @typescript-eslint/no-redeclare
export type SyncMetricOption = (typeof SyncMetricOption)[keyof typeof SyncMetricOption];

interface SyncMetricListboxProps {
  selectedValue: SyncMetricOption;
  onSelect: (selectedValue: SyncMetricOption) => void;
}

export const SyncMetricListbox: React.FC<SyncMetricListboxProps> = ({ selectedValue, onSelect }) => {
  return (
    <StreamsListHeaderListbox<SyncMetricOption>
      options={[
        {
          label: <FormattedMessage id="connection.stream.status.table.latestSync.bytes" />,
          value: SyncMetricOption.bytes,
        },
        {
          label: <FormattedMessage id="connection.stream.status.table.latestSync.records" />,
          value: SyncMetricOption.records,
        },
      ]}
      selectedValue={selectedValue}
      adaptiveWidth={false}
      onSelect={onSelect}
    />
  );
};
