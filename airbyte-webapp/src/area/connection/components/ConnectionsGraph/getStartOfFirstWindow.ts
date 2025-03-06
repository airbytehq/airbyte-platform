import dayjs, { Dayjs } from "dayjs";

import { LookbackWindow, lookbackConfigs } from "./lookbackConfiguration";

export const getStartOfFirstWindow = (lookback: LookbackWindow): Dayjs => {
  const { windowLength, totalLength, unit } = lookbackConfigs[lookback];

  const now = dayjs();
  // Get back to the start of the current window, which may be in progress (e.g. if it's 11:13 we want to start counting
  // back from 11:00 so we have even windows)
  const partialCurrentWindow = now.get(unit) % windowLength;

  return now.subtract(partialCurrentWindow, unit).subtract(totalLength, unit).startOf(unit);
};
