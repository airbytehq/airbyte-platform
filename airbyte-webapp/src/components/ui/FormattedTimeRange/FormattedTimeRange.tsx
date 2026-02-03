import { FormatDateOptions, FormattedMessage, useIntl } from "react-intl";

interface FormattedTimeRangeProps {
  from: Date;
  to: Date;
}

const TIME_FORMAT: FormatDateOptions = { hour: "numeric", minute: "numeric" };

/**
 * formatjs has a <FormattedDateTimeRange> component, but that will always display the date if the times span more than
 * a day. This is not desirable for time ranges like 11:00 PM - 12:00 AM, which are technically two different days. This
 * component allows us to display this time range without showing the date.
 */
export const FormattedTimeRange: React.FC<FormattedTimeRangeProps> = ({ from, to }) => {
  const { formatTime } = useIntl();

  return (
    <FormattedMessage
      id="general.timeRange"
      values={{ from: formatTime(from, TIME_FORMAT), to: formatTime(to, TIME_FORMAT) }}
    />
  );
};
