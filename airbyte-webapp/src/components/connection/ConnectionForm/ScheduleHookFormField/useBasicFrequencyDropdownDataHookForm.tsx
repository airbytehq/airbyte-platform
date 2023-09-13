import { useMemo } from "react";
import { useIntl } from "react-intl";

import { Option } from "components/ui/ListBox";

import { ConnectionScheduleDataBasicSchedule, WebBackendConnectionRead } from "core/request/AirbyteClient";

export const BASIC_FREQUENCY_DEFAULT_VALUE: ConnectionScheduleDataBasicSchedule = { units: 24, timeUnit: "hours" };
const frequencyConfig: ConnectionScheduleDataBasicSchedule[] = [
  {
    units: 1,
    timeUnit: "hours",
  },
  {
    units: 2,
    timeUnit: "hours",
  },
  {
    units: 3,
    timeUnit: "hours",
  },
  {
    units: 6,
    timeUnit: "hours",
  },
  {
    units: 8,
    timeUnit: "hours",
  },
  {
    units: 12,
    timeUnit: "hours",
  },
  BASIC_FREQUENCY_DEFAULT_VALUE,
];

export const useBasicFrequencyDropdownDataHookForm = (
  additionalFrequency: WebBackendConnectionRead["scheduleData"]
): Array<Option<ConnectionScheduleDataBasicSchedule>> => {
  const { formatMessage } = useIntl();

  return useMemo(() => {
    const frequencies = [...frequencyConfig];

    // TODO: ask team what is additionalFrequency
    if (additionalFrequency?.basicSchedule) {
      const additionalFreqAlreadyPresent = frequencies.some(
        (frequency) =>
          frequency?.timeUnit === additionalFrequency.basicSchedule?.timeUnit &&
          frequency?.units === additionalFrequency.basicSchedule?.units
      );
      if (!additionalFreqAlreadyPresent) {
        frequencies.push(additionalFrequency.basicSchedule);
      }
    }

    return frequencies.map((frequency) => ({
      label: formatMessage(
        {
          id: `form.every.${frequency.timeUnit}`,
        },
        { value: frequency.units }
      ),
      value: frequency,
    }));
  }, [additionalFrequency, formatMessage]);
};
