import classNames from "classnames";
import en from "date-fns/locale/en-US";
import dayjs from "dayjs";
import React, { useCallback, useMemo, useRef } from "react";
import ReactDatePicker, { registerLocale } from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import { useIntl } from "react-intl";

import { CustomHeader } from "./CustomHeader";
import styles from "./DatePicker.module.scss";
import {
  ISO8601_NO_MILLISECONDS,
  ISO8601_WITH_MICROSECONDS,
  ISO8601_WITH_MILLISECONDS,
  toEquivalentLocalTime,
  YEAR_MONTH_DAY_FORMAT,
  YEAR_MONTH_FORMAT,
} from "./utils";
import { Button } from "../Button";
import { Input } from "../Input";

export interface DatePickerProps {
  id?: string;
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
  yearMonth?: boolean;
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
      icon="calendar"
    />
  );
});
DatepickerButton.displayName = "DatepickerButton";

// Additional locales can be registered here as necessary
registerLocale("en-US", en);
registerLocale("en-GB", en);
registerLocale("en", en);

export const DatePicker: React.FC<DatePickerProps> = ({
  id,
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
  yearMonth,
}) => {
  const { locale, formatMessage } = useIntl();
  const datepickerRef = useRef<ReactDatePicker>(null);

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
        : yearMonth
        ? date.format(YEAR_MONTH_FORMAT)
        : date.format(YEAR_MONTH_DAY_FORMAT);
      onChange(formattedDate);
      inputRef.current?.focus();
    },
    [withTime, datetimeFormat, yearMonth, onChange]
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
        id={id}
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
          renderCustomHeader={(props) => <CustomHeader {...props} showMonthDropdown showYearDropdown />}
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
          showPopperArrow={false}
          showTimeSelect={withTime}
          startDate={startDate}
          timeCaption={formatMessage({ id: "form.datepickerTimeCaption" })}
          showMonthYearPicker={yearMonth}
        />
      </div>
    </div>
  );
};
