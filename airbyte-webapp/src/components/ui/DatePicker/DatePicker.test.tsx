import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import dayjs from "dayjs";

import { TestWrapper } from "test-utils/testutils";

import { DatePicker, toEquivalentLocalTime } from "./DatePicker";

describe("Timezones", () => {
  it("should always be US/Pacific", () => {
    expect(process.env.TZ).toEqual("US/Pacific");
  });
});

describe(`${toEquivalentLocalTime.name}`, () => {
  beforeAll(() => {
    jest.useFakeTimers().setSystemTime(new Date("2000-01-01T00:00:00"));
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  // Seems silly, but dayjs has a bug when formatting years, so this is a useful test:
  // https://github.com/iamkun/dayjs/issues/1745
  it("handles a date in the year 1", () => {
    const TEST_UTC_TIMESTAMP = "0001-12-01T09:00:00Z";

    const result = toEquivalentLocalTime(TEST_UTC_TIMESTAMP);

    expect(result).toEqual(undefined);
  });

  it("handles an invalid date", () => {
    const TEST_UTC_TIMESTAMP = "not a date";

    const result = toEquivalentLocalTime(TEST_UTC_TIMESTAMP);

    expect(result).toEqual(undefined);
  });

  it("outputs the same YYYY-MM-DDTHH:mm:ss", () => {
    const TEST_TIMEZONE_UTC_OFFSET_IN_MINUTES = dayjs().utcOffset() * -1; // corresponds to our global test timezone of US/Pacific, and accounts for dayjs using a negative offset
    const TEST_UTC_TIMESTAMP = "2000-01-01T12:00:00Z";

    const result = toEquivalentLocalTime(TEST_UTC_TIMESTAMP);

    // Regardless of the timezone, the local time should be the same
    expect(dayjs(result).format().substring(0, 19)).toEqual(TEST_UTC_TIMESTAMP.substring(0, 19));
    expect(result?.getTimezoneOffset()).toEqual(TEST_TIMEZONE_UTC_OFFSET_IN_MINUTES);
  });
});

describe(`${DatePicker.name}`, () => {
  let user: ReturnType<(typeof userEvent)["setup"]>;

  beforeAll(() => {
    // Since we're using jest's fake timers we need to make sure userEvent understands how to advance them
    user = userEvent.setup({ advanceTimers: jest.advanceTimersByTime });
    jest.useFakeTimers().setSystemTime(new Date("2010-09-05T00:00:00"));
  });

  afterAll(() => {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
  });

  it("allows typing a date manually", async () => {
    const MOCK_DESIRED_DATETIME = "2010-09-12T00:00:00Z";
    let mockValue = "";
    render(
      <TestWrapper>
        <DatePicker
          onChange={(value) => {
            // necessary for controlled inputs https://github.com/testing-library/user-event/issues/387#issuecomment-819868799
            mockValue = mockValue + value;
          }}
          value={mockValue}
        />
      </TestWrapper>
    );

    const input = screen.getByTestId("input");
    await user.type(input, MOCK_DESIRED_DATETIME);

    expect(mockValue).toEqual(MOCK_DESIRED_DATETIME);
  });

  it("allows selecting a date from the datepicker", async () => {
    const MOCK_DESIRED_DATE = "2010-09-12";
    let mockValue = "";
    render(
      <TestWrapper>
        <DatePicker
          onChange={(value) => {
            // necessary for controlled inputs https://github.com/testing-library/user-event/issues/387#issuecomment-819868799
            mockValue = mockValue + value;
          }}
          value={mockValue}
        />
      </TestWrapper>
    );

    const datepicker = screen.getByLabelText("Open datepicker");
    await user.click(datepicker);
    const date = screen.getByLabelText("Choose Sunday, September 12th, 2010");
    await user.click(date);

    expect(mockValue).toEqual(MOCK_DESIRED_DATE);
  });

  it("allows selecting a datetime from the datepicker", async () => {
    const MOCK_DESIRED_DATETIME = "2010-09-05T12:00:00Z";
    let mockValue = "";
    render(
      <TestWrapper>
        <DatePicker
          onChange={(value) => {
            // necessary for controlled inputs https://github.com/testing-library/user-event/issues/387#issuecomment-819868799
            mockValue = mockValue + value;
          }}
          value={mockValue}
          withTime
        />
      </TestWrapper>
    );

    const datepicker = screen.getByLabelText("Open datepicker");
    await user.click(datepicker);
    const date = screen.getByText("12:00 PM");
    await user.click(date);

    expect(mockValue).toEqual(MOCK_DESIRED_DATETIME);
  });

  it("allows selecting a datetime with milliseconds from the datepicker", async () => {
    const MOCK_DESIRED_DATETIME = "2010-09-05T12:00:00.000Z";
    let mockValue = "";
    render(
      <TestWrapper>
        <DatePicker
          onChange={(value) => {
            // necessary for controlled inputs https://github.com/testing-library/user-event/issues/387#issuecomment-819868799
            mockValue = mockValue + value;
          }}
          value={mockValue}
          withTime
          withPrecision="milliseconds"
        />
      </TestWrapper>
    );

    const datepicker = screen.getByLabelText("Open datepicker");
    await user.click(datepicker);
    const date = screen.getByText("12:00 PM");
    await user.click(date);

    expect(mockValue).toEqual(MOCK_DESIRED_DATETIME);
  });

  it("allows selecting a datetime with microseconds from the datepicker", async () => {
    const MOCK_DESIRED_DATETIME = "2010-09-05T12:00:00.000000Z";
    let mockValue = "";
    render(
      <TestWrapper>
        <DatePicker
          onChange={(value) => {
            // necessary for controlled inputs https://github.com/testing-library/user-event/issues/387#issuecomment-819868799
            mockValue = mockValue + value;
          }}
          value={mockValue}
          withTime
          withPrecision="microseconds"
        />
      </TestWrapper>
    );

    const datepicker = screen.getByLabelText("Open datepicker");
    await user.click(datepicker);
    const date = screen.getByText("12:00 PM");
    await user.click(date);

    expect(mockValue).toEqual(MOCK_DESIRED_DATETIME);
  });

  it("focuses the input after selecting a date from the datepicker", async () => {
    let mockValue = "";
    render(
      <TestWrapper>
        <DatePicker onChange={(value) => (mockValue = value)} value={mockValue} />
      </TestWrapper>
    );

    const datepicker = screen.getByLabelText("Open datepicker");
    await user.click(datepicker);
    const date = screen.getByLabelText("Choose Sunday, September 12th, 2010");
    await user.click(date);

    const input = screen.getByTestId("input");

    expect(input).toHaveFocus();
  });
});
