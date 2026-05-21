import { useMemo } from "react";
import { useIntl } from "react-intl";

import { Option } from "components/ui/ListBox";

import { ConnectorIds } from "area/connector/utils";
import { useOrganizationPlan } from "area/organization/utils";
import { PlanAvailability } from "cloud/area/billing/components/PlanAvailabilityBadges";
import { ConnectionScheduleDataBasicSchedule, WebBackendConnectionRead } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";

export const BASIC_FREQUENCY_DEFAULT_VALUE: ConnectionScheduleDataBasicSchedule = { units: 24, timeUnit: "hours" };
const PLUS_OR_PRO_PLAN_AVAILABILITY: PlanAvailability[] = ["plus", "pro"];

export interface BasicFrequencyOption extends Option<ConnectionScheduleDataBasicSchedule> {
  availableInPlans?: PlanAvailability[];
}

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

export const SOURCE_SPECIFIC_FREQUENCY_DEFAULT: Record<string, ConnectionScheduleDataBasicSchedule> = {
  [ConnectorIds.Sources.MongoDb]: { units: 6, timeUnit: "hours" },
};

export const useBasicFrequencyDropdownData = (
  additionalFrequency: WebBackendConnectionRead["scheduleData"]
): BasicFrequencyOption[] => {
  const { formatMessage } = useIntl();
  const isSyncFrequencyUnderOneHourAllowed = useFeature(FeatureItem.AllowSyncFrequencyUnderOneHour);
  const { isUnifiedTrialPlan, isStandardTrialPlan, isStandardPlan } = useOrganizationPlan();
  const showPlusOrProAvailability = isUnifiedTrialPlan || isStandardTrialPlan || isStandardPlan;

  return useMemo(() => {
    // Conditionally add 15 and 30 minute options when feature flag is enabled
    const frequencies = isSyncFrequencyUnderOneHourAllowed
      ? [{ units: 15, timeUnit: "minutes" as const }, { units: 30, timeUnit: "minutes" as const }, ...frequencyConfig]
      : [...frequencyConfig];

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

    return frequencies.map((frequency) => {
      const isMinuteFrequency = frequency.timeUnit === "minutes";

      return {
        label: formatMessage(
          {
            id: `form.every.${frequency.timeUnit}`,
          },
          { value: frequency.units }
        ),
        value: frequency,
        "data-testid": `frequency-${frequency.units}-${frequency.timeUnit}`,
        ...(isMinuteFrequency &&
          showPlusOrProAvailability && {
            availableInPlans: PLUS_OR_PRO_PLAN_AVAILABILITY,
          }),
      };
    });
  }, [additionalFrequency, formatMessage, isSyncFrequencyUnderOneHourAllowed, showPlusOrProAvailability]);
};
