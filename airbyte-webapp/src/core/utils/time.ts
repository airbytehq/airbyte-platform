import type { Dayjs, ManipulateType } from "dayjs";

import dayjs from "dayjs";
import { useIntl } from "react-intl";

/**
 * Given a `time`, this effectively increments the time forward in steps of `${value}${unit}` until the future is reached (I hope there's flying cars!
 * e.g. to find the future from 2 months ago in 6 month steps:
 * time=dayjs().subtract(2, "month"), value=6, unit="month"
 * which would be 4 months from now
 *
 * If time is already in the future, it is simply returned. No attempt is made to normalize the time to the step.
 */
export const moveTimeToFutureByPeriod = (time: Dayjs, value: number, unit: ManipulateType) => {
  const now = Date.now();
  if (time.valueOf() < now) {
    const timeDiff = now - time.valueOf();
    const scheduleInMs = dayjs.duration(value, unit).asMilliseconds();
    const msNeeded = Math.ceil(timeDiff / scheduleInMs) * scheduleInMs;
    return time.add(msNeeded, "millisecond");
  }
  return time;
};

export const useFormatLengthOfTime = (lengthOfTimeMs: number) => {
  const { formatMessage } = useIntl();

  const start = dayjs(0);
  const end = dayjs(lengthOfTimeMs);
  const hours = Math.abs(end.diff(start, "hour"));
  const minutes = Math.abs(end.diff(start, "minute")) - hours * 60;
  const seconds = Math.abs(end.diff(start, "second")) - minutes * 60 - hours * 3600;

  const strHours = hours ? formatMessage({ id: "sources.hour" }, { hour: hours }) : "";
  const strMinutes = hours || minutes ? formatMessage({ id: "sources.minute" }, { minute: minutes }) : "";
  const strSeconds = formatMessage({ id: "sources.second" }, { second: seconds });
  return `${strHours}${strMinutes}${strSeconds}`;
};
