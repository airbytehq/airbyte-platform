import { act, render, screen } from "@testing-library/react";

import { DataplanePulseGraph } from "./DataplanePulseGraph";

const mockUseIntl = jest.fn(() => ({ formatMessage: ({ id }: { id: string }) => id }));

jest.mock("react-intl", () => ({
  ...jest.requireActual("react-intl"),
  useIntl: () => mockUseIntl(),
}));

const NOW = new Date("2026-10-23T12:00:00.000Z").getTime();
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const UPDATE_INTERVAL = 6 * SECOND;

const heartbeatAgo = (ageMs: number) => ({ timestamp: NOW - ageMs });

const advanceByIntervals = (count: number) => {
  for (let i = 0; i < count; i++) {
    act(() => {
      jest.advanceTimersByTime(UPDATE_INTERVAL);
    });
  }
};

const getSpikes = (container: HTMLElement) => Array.from(container.querySelectorAll("line"));
const getSpikePositions = (container: HTMLElement) => getSpikes(container).map((spike) => spike.style.transform);

describe(`${DataplanePulseGraph.displayName}`, () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(NOW);
    mockUseIntl.mockClear();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("should show the no-data message when there are no heartbeats", () => {
    render(<DataplanePulseGraph heartbeats={[]} />);

    expect(screen.getByText("dataplaneHealth.noData")).toBeInTheDocument();
  });

  it("should render one spike for each heartbeat in the five minute window", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE), heartbeatAgo(2 * MINUTE)]} />);

    expect(getSpikes(container)).toHaveLength(2);
  });

  it("should not render heartbeats older than the five minute window", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE), heartbeatAgo(6 * MINUTE)]} />);

    expect(getSpikes(container)).toHaveLength(1);
  });

  it("should place a heartbeat from one minute ago at 40px of the 50px graph", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);

    expect(getSpikePositions(container)).toEqual(["translateX(40px)"]);
  });

  it("should order the spikes from the oldest heartbeat to the newest", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE), heartbeatAgo(4 * MINUTE)]} />);

    expect(getSpikePositions(container)).toEqual(["translateX(10px)", "translateX(40px)"]);
  });

  it("should fade a heartbeat the older it gets", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(4 * MINUTE), heartbeatAgo(MINUTE)]} />);

    expect(getSpikes(container).map((spike) => Number(spike.style.opacity))).toEqual([
      expect.closeTo(0.36),
      expect.closeTo(0.84),
    ]);
  });

  it("should animate the spikes over the same duration as the update interval", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);

    expect(getSpikes(container)[0].style.transitionDuration).toBe(`${UPDATE_INTERVAL}ms`);
  });

  it("should keep the spikes in place until the update interval has passed", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);

    act(() => {
      jest.advanceTimersByTime(UPDATE_INTERVAL - 1);
    });

    expect(getSpikePositions(container)).toEqual(["translateX(40px)"]);
  });

  it("should move the spikes by one pixel when the update interval has passed", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);

    act(() => {
      jest.advanceTimersByTime(UPDATE_INTERVAL);
    });

    expect(getSpikePositions(container)).toEqual(["translateX(39px)"]);
  });

  it("should drop a heartbeat once it slides out of the window", () => {
    const { container } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(5 * MINUTE - UPDATE_INTERVAL + 1)]} />);

    advanceByIntervals(1);

    expect(getSpikes(container)).toHaveLength(0);
  });

  describe("when a heartbeat arrives between two updates", () => {
    // The graph last updated at NOW, and a poll then delivers a heartbeat that is newer than that update.
    const renderWithHeartbeatArrivingBetweenUpdates = () => {
      const { container, rerender } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);

      act(() => {
        jest.advanceTimersByTime(3 * SECOND);
      });
      rerender(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE), { timestamp: NOW + SECOND }]} />);

      return container;
    };

    it("should show it right away", () => {
      const container = renderWithHeartbeatArrivingBetweenUpdates();

      expect(getSpikes(container)).toHaveLength(2);
    });

    it("should draw it at full opacity", () => {
      const container = renderWithHeartbeatArrivingBetweenUpdates();

      expect(Number(getSpikes(container)[1].style.opacity)).toBe(1);
    });
  });

  it("should not render before the update interval has passed", () => {
    render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);
    mockUseIntl.mockClear();

    act(() => {
      jest.advanceTimersByTime(UPDATE_INTERVAL - 1);
    });

    expect(mockUseIntl).not.toHaveBeenCalled();
  });

  it("should render once for each update interval that passes", () => {
    render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);
    mockUseIntl.mockClear();

    advanceByIntervals(10);

    expect(mockUseIntl).toHaveBeenCalledTimes(10);
  });

  it("should not render again when the parent renders with the same heartbeats", () => {
    const heartbeats = [heartbeatAgo(MINUTE)];
    const { rerender } = render(<DataplanePulseGraph heartbeats={heartbeats} />);
    mockUseIntl.mockClear();

    rerender(<DataplanePulseGraph heartbeats={heartbeats} />);

    expect(mockUseIntl).not.toHaveBeenCalled();
  });

  it("should render again when the heartbeats change", () => {
    const { rerender } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);
    mockUseIntl.mockClear();

    rerender(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE), heartbeatAgo(30 * SECOND)]} />);

    expect(mockUseIntl).toHaveBeenCalledTimes(1);
  });

  it("should stop its update timer when it unmounts", () => {
    const { unmount } = render(<DataplanePulseGraph heartbeats={[heartbeatAgo(MINUTE)]} />);

    unmount();

    expect(jest.getTimerCount()).toBe(0);
  });
});
