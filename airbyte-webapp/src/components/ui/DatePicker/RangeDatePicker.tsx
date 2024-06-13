import dayjs from "dayjs";
import React, { forwardRef, useCallback } from "react";
import ReactDatePicker from "react-datepicker";
import { useIntl } from "react-intl";

import { CustomHeader } from "./CustomHeader";
import styles from "./RangeDatePicker.module.scss";
import { MONTH_DAY } from "./utils";
import { Button, ButtonProps } from "../Button";
import { FlexContainer } from "../Flex";
import { Text } from "../Text";

export interface RangeDatePickerProps {
  onChange: (dates: [string, string]) => void;
  value: [string, string];
  minDate?: string;
  maxDate?: string;
  disabled?: boolean;
  dateFormat?: string;
  buttonText?: string;
}

export const RangeDatePicker: React.FC<RangeDatePickerProps> = ({
  minDate,
  maxDate,
  onChange,
  value,
  dateFormat = MONTH_DAY,
  buttonText = "form.rangeDatePicker.selectDate",
}: RangeDatePickerProps) => {
  const { locale, formatMessage } = useIntl();

  const handleDatepickerChange = useCallback(
    (value: [Date | null, Date | null]) => {
      const [dateFrom, dateTo] = value;

      const dateFromStr = dateFrom ? dayjs(dateFrom).format("YYYY-MM-DD") : "";
      const dateToStr = dateTo ? dayjs(dateTo).format("YYYY-MM-DD") : "";

      onChange([dateFromStr, dateToStr]);
    },
    [onChange]
  );

  const CustomInput = forwardRef<HTMLButtonElement, ButtonProps>(({ onClick }, ref) => {
    const dateFrom = value[0] ? dayjs(value[0]).format(dateFormat) : "";
    const dateTo = value[1] ? dayjs(value[1]).format(dateFormat) : "";

    return (
      <Button variant="clear" className={styles.button} icon="calendar" onClick={onClick} type="button" ref={ref}>
        <FlexContainer alignItems="center" gap="xs">
          {dateFrom ? (
            <>
              <Text color={dateFrom ? "grey500" : "grey300"} bold>
                {dateFrom ? dateFrom : formatMessage({ id: "form.rangeDatePicker.selectDate" })}
              </Text>
              <span className={styles.hyphen}>-</span>
              <Text color={dateTo ? "grey500" : "grey300"} bold>
                {dateTo ? dateTo : formatMessage({ id: "form.rangeDatePicker.selectDate" })}
              </Text>
            </>
          ) : (
            <Text color="grey400" bold>
              {formatMessage({ id: buttonText })}
            </Text>
          )}
        </FlexContainer>
      </Button>
    );
  });
  CustomInput.displayName = "CustomInput";

  return (
    <div className={styles.container}>
      <ReactDatePicker
        selectsRange
        monthsShown={2}
        customInput={<CustomInput />}
        renderCustomHeader={(props) => <CustomHeader {...props} selectsRange />}
        locale={locale}
        onChange={handleDatepickerChange}
        startDate={value[0] ? dayjs(value[0]).toDate() : null}
        endDate={value[1] ? dayjs(value[1]).toDate() : null}
        maxDate={maxDate ? dayjs(maxDate).toDate() : null}
        minDate={minDate ? dayjs(minDate).toDate() : null}
      />
    </div>
  );
};
