import React from "react";
import { FormattedMessage } from "react-intl";

import { StreamsListHeaderListbox } from "./StreamsListHeaderListbox";

export const DateMetricOption = {
  absolute: "absolute",
  relative: "relative",
} as const;

// eslint-disable-next-line @typescript-eslint/no-redeclare
export type DateMetricOption = (typeof DateMetricOption)[keyof typeof DateMetricOption];

interface DateMetricListboxProps {
  selectedValue: DateMetricOption;
  onSelect: (selectedValue: DateMetricOption) => void;
}

export const DateMetricListbox: React.FC<DateMetricListboxProps> = ({ selectedValue, onSelect }) => {
  return (
    <StreamsListHeaderListbox<DateMetricOption>
      options={[
        {
          label: <FormattedMessage id="connection.stream.status.table.dataFreshAsOf.absolute" />,
          value: DateMetricOption.absolute,
        },
        {
          label: <FormattedMessage id="connection.stream.status.table.dataFreshAsOf.relative" />,
          value: DateMetricOption.relative,
        },
      ]}
      selectedValue={selectedValue}
      adaptiveWidth={false}
      onSelect={onSelect}
    />
  );
};
