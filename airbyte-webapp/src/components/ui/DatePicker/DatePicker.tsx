import classNames from "classnames";
import en from "date-fns/locale/en-US";
import dayjs from "dayjs";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import ReactDatePicker, { registerLocale } from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { useIntl } from "react-intl";

import { Icon } from "components/ui/Icon";

import styles from "./DatePicker.module.scss";
import { Button } from "../Button";
import { Input } from "../Input";

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

export interface DatePickerProps {
  className?: string;
  disabled?: boolean;
  endDate?: Date;
  error?: boolean;
  maxDate?: string;
  minDate?: string;
  onBlur?: () => void;
  onChange: (value: string) => void;
  onFocus?: () => void;
  placeholder?: string;
  readOnly?: boolean;
  selectsEnd?: true;
  selectsStart?: true;
  startDate?: Date;
  value: string;
  withPrecision?: "milliseconds" | "microseconds";
  withTime?: boolean;
}

interface DatePickerButtonTriggerProps {
  onClick?: () => void;
}

const DatepickerButton = React.forwardRef<HTMLButtonElement, DatePickerButtonTriggerProps>(({ onClick }, ref) => {
  const { formatMessage } = useIntl();

  return (
    <Button
      className={styles.datepickerButton}
      aria-label={formatMessage({ id: "form.openDatepicker" })}
      onClick={onClick}
      ref={ref}
      type="button"
      variant="clear"
      icon={<Icon type="calendar" />}
    />
  );
});
DatepickerButton.displayName = "DatepickerButton";

const ISO8601_NO_MILLISECONDS = "YYYY-MM-DDTHH:mm:ss[Z]";
const ISO8601_WITH_MILLISECONDS = "YYYY-MM-DDTHH:mm:ss.SSS[Z]";
const ISO8601_WITH_MICROSECONDS = "YYYY-MM-DDTHH:mm:ss.SSSSSS[Z]";
const YEAR_MONTH_DAY_FORMAT = "YYYY-MM-DD";

export const DatePicker: React.FC<DatePickerProps> = ({
  className,
  disabled,
  endDate,
  error,
  maxDate = "",
  minDate = "",
  onBlur,
  onChange,
  onFocus,
  placeholder,
  readOnly,
  selectsEnd,
  selectsStart,
  startDate,
  value = "",
  withPrecision,
  withTime = false,
}) => {
  const { locale, formatMessage } = useIntl();
  const datepickerRef = useRef<ReactDatePicker>(null);

  // Additional locales can be registered here as necessary
  useEffect(() => {
    switch (locale) {
      case "en":
        registerLocale(locale, en);
        break;
    }
  }, [locale]);

  const inputRef = useRef<HTMLInputElement>(null);

  const datetimeFormat = useMemo(
    () =>
      withPrecision === "milliseconds"
        ? ISO8601_WITH_MILLISECONDS
        : withPrecision === "microseconds"
        ? ISO8601_WITH_MICROSECONDS
        : ISO8601_NO_MILLISECONDS,
    [withPrecision]
  );

  const handleDatepickerChange = useCallback(
    (val: Date | null) => {
      // Workaround for a bug in react-datepicker where selecting a time does not zero out the millisecond value https://github.com/Hacker0x01/react-datepicker/issues/1991
      val?.setMilliseconds(0);
      const date = dayjs(val);
      if (!date.isValid()) {
        onChange("");
        return;
      }
      const formattedDate = withTime
        ? date.utcOffset(0, true).format(datetimeFormat)
        : date.format(YEAR_MONTH_DAY_FORMAT);
      onChange(formattedDate);
      inputRef.current?.focus();
    },
    [onChange, withTime, datetimeFormat]
  );

  const handleInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      onChange(e.target.value);
    },
    [onChange]
  );

  const localDate = useMemo(() => toEquivalentLocalTime(value), [value]);
  const localMinDate = useMemo(() => toEquivalentLocalTime(minDate), [minDate]);
  const localMaxDate = useMemo(() => toEquivalentLocalTime(maxDate), [maxDate]);

  const wrapperRef = useRef<HTMLDivElement>(null);
  const handleWrapperBlur = () => {
    if (onBlur && !wrapperRef.current?.matches(":focus-within")) {
      onBlur();
    }
  };

  return (
    <div className={classNames(styles.wrapper, className)} ref={wrapperRef} onBlur={handleWrapperBlur}>
      <Input
        placeholder={placeholder}
        error={error}
        value={value}
        onChange={handleInputChange}
        onFocus={() => {
          datepickerRef.current?.setOpen(true);
          onFocus?.();
        }}
        className={styles.input}
        ref={inputRef}
        disabled={disabled}
        readOnly={readOnly}
      />
      <div className={styles.datepickerButtonContainer}>
        <ReactDatePicker
          customInput={<DatepickerButton />}
          disabled={disabled}
          dropdownMode="select"
          endDate={endDate}
          locale={locale}
          maxDate={localMaxDate}
          minDate={localMinDate}
          onChange={handleDatepickerChange}
          popperClassName={styles.popup}
          popperPlacement="bottom-end"
          portalId="react-datepicker"
          readOnly={readOnly}
          ref={datepickerRef}
          selected={localDate}
          selectsEnd={selectsEnd}
          selectsStart={selectsStart}
          showMonthDropdown
          showPopperArrow={false}
          showTimeSelect={withTime}
          showYearDropdown
          startDate={startDate}
          timeCaption={formatMessage({ id: "form.datepickerTimeCaption" })}
        />
      </div>
    </div>
  );
};
