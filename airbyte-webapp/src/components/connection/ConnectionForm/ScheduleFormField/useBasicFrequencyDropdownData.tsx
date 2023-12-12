import { useMemo } from "react";
import { useIntl } from "react-intl";

import { Option } from "components/ui/ListBox";

import { ConnectionScheduleDataBasicSchedule, WebBackendConnectionRead } from "core/api/types/AirbyteClient";

export const BASIC_FREQUENCY_DEFAULT_VALUE: ConnectionScheduleDataBasicSchedule = { units: 24, timeUnit: "hours" };
export const frequencyConfig: ConnectionScheduleDataBasicSchedule[] = [
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

export const useBasicFrequencyDropdownData = (
  additionalFrequency: WebBackendConnectionRead["scheduleData"]
): Array<Option<ConnectionScheduleDataBasicSchedule>> => {
  const { formatMessage } = useIntl();

  return useMemo(() => {
    const frequencies = [...frequencyConfig];

    /**
     * There's a possibility that users have created custom frequencies via the API,
     * so we need to append them to the default frequency list.
     */
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
      "data-testid": `frequency-${frequency.units}-${frequency.timeUnit}`,
    }));
  }, [additionalFrequency, formatMessage]);
};
