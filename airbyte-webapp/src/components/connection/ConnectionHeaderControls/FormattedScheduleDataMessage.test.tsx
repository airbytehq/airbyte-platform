import { render } from "@testing-library/react";

import { TestWrapper } from "test-utils";

import { ConnectionScheduleData, ConnectionScheduleDataBasicScheduleTimeUnit } from "core/api/types/AirbyteClient";

import { FormattedScheduleDataMessage, FormattedScheduleDataMessageProps } from "./FormattedScheduleDataMessage";

describe("FormattedScheduleDataMessage", () => {
  const renderComponent = (props: FormattedScheduleDataMessageProps) => {
    return render(
      <TestWrapper>
        <FormattedScheduleDataMessage {...props} />
      </TestWrapper>
    );
  };

  it("should render 'Manual' schedule type if scheduleData wasn't provided", () => {
    const { getByText } = renderComponent({ scheduleType: "manual" });
    expect(getByText("Manual")).toBeInTheDocument();
  });

  it("should render '24 hours' schedule type", () => {
    const scheduleData = {
      basicSchedule: {
        units: 24,
        timeUnit: "hours" as ConnectionScheduleDataBasicScheduleTimeUnit,
      },
    };
    const { getByText } = renderComponent({ scheduleType: "basic", scheduleData });
    expect(getByText("Every 24 hours")).toBeInTheDocument();
  });

  it("should render 'Cron' schedule type with humanized format", () => {
    const scheduleData = {
      cron: {
        cronExpression: "0 0 14 ? * THU" as string,
        cronTimeZone: "UTC",
      },
    };
    const { getByText } = renderComponent({ scheduleType: "cron", scheduleData });
    expect(getByText("At 02:00 PM, only on Thursday")).toBeInTheDocument();
  });

  it("should NOT render anything", () => {
    const scheduleData = {
      basic: {
        units: 24,
        timeUnit: "hours" as ConnectionScheduleDataBasicScheduleTimeUnit,
      },
    };
    const { queryByText } = renderComponent({
      scheduleType: "cron",
      scheduleData: scheduleData as unknown as ConnectionScheduleData, // for testing purposes
    });
    expect(queryByText("24")).toBeNull();
  });
});
