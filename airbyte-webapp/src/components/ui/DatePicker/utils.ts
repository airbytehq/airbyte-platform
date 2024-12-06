import dayjs from "dayjs";

export const ISO8601_NO_MILLISECONDS = "YYYY-MM-DDTHH:mm:ss[Z]";
export const ISO8601_WITH_MILLISECONDS = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";
export const ISO8601_WITH_MICROSECONDS = "YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]";
export const YEAR_MONTH_DAY_FORMAT = "YYYY-MM-DD";
export const YEAR_MONTH_FORMAT = "YYYY-MM";
export const MONTH_DAY = "MMMM D";

/**
 * Converts a UTC string into a JS Date object with the same local time
 *
 * Necessary because react-datepicker does not allow us to set the timezone to UTC, only the current browser time.
 * In order to display the UTC timezone in the datepicker, we need to convert it into the local time:
 *
 * 2022-01-01T09:00:00Z       - the UTC format that airbyte-server expects (e.g. 9:00am)
 * 2022-01-01T10:00:00+01:00  - what react-datepicker might convert this date into and display (e.g. 10:00am - bad!)
 * 2022-01-01T09:00:00+01:00  - what we give react-datepicker instead, to trick it (User sees 9:00am - good!)
 */
export const toEquivalentLocalTime = (utcString: string): Date | undefined => {
  if (!utcString) {
    return undefined;
  }

  const date = dayjs.utc(utcString);

  if (!date?.isValid()) {
    return undefined;
  }

  // Get the user's UTC offset based on the local timezone and the given date
  const browserUtcOffset = dayjs(utcString).utcOffset();

  // Convert the selected date into a string which we can use to initialize a new date object.
  // The second parameter to utcOffset() keeps the same local time, only changing the timezone.
  const localDateAsString = date.utcOffset(browserUtcOffset, true).format();

  const equivalentDate = dayjs(localDateAsString);

  // dayjs does not 0-pad years when formatting, so it's possible to have an invalid date here
  // https://github.com/iamkun/dayjs/issues/1745
  if (!equivalentDate.isValid()) {
    return undefined;
  }

  return equivalentDate.toDate();
};
