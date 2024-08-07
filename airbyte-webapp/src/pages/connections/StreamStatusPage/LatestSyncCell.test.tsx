import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { render } from "test-utils";

import { LatestSyncCell } from "./LatestSyncCell";

const BASE_TIME = 1719860400000;

jest.useFakeTimers().setSystemTime(BASE_TIME);

describe("LastSyncCell", () => {
  it("past sync", async () => {
    const result = await render(
      <LatestSyncCell
        status={ConnectionStatusIndicatorStatus.Synced}
        recordsLoaded={1000}
        recordsExtracted={1000}
        syncStartedAt={BASE_TIME - 1000}
        isLoadingHistoricalData={false}
      />
    );

    expect(result.container.textContent).toBe("1,000 loaded");
  });

  it("past sync without recordsLoaded", async () => {
    const result = await render(
      <LatestSyncCell
        status={ConnectionStatusIndicatorStatus.Synced}
        recordsLoaded={undefined}
        recordsExtracted={undefined}
        syncStartedAt={BASE_TIME - 1000}
        isLoadingHistoricalData={false}
      />
    );

    expect(result.container.textContent).toBe("-");
  });

  it("extracted == 0 && loaded == 0", async () => {
    const result = await render(
      <LatestSyncCell
        status={ConnectionStatusIndicatorStatus.Syncing}
        recordsLoaded={0}
        recordsExtracted={0}
        syncStartedAt={BASE_TIME - 100_000}
        isLoadingHistoricalData={false}
      />
    );

    expect(result.container.textContent).toBe("Starting... | 1m elapsed");
  });

  it("extracted > 0 && loaded == 0", async () => {
    const result = await render(
      <LatestSyncCell
        status={ConnectionStatusIndicatorStatus.Syncing}
        recordsLoaded={0}
        recordsExtracted={5000}
        syncStartedAt={BASE_TIME - 130_000}
        isLoadingHistoricalData={false}
      />
    );

    expect(result.container.textContent).toBe("5,000 extracted | 2m elapsed");
  });

  it("extracted > 0 && loaded > 0", async () => {
    const result = await render(
      <LatestSyncCell
        status={ConnectionStatusIndicatorStatus.Syncing}
        recordsLoaded={3000}
        recordsExtracted={5000}
        syncStartedAt={BASE_TIME - 130_000}
        isLoadingHistoricalData={false}
      />
    );

    expect(result.container.textContent).toBe("3,000 loaded | 2m elapsed");
  });

  it.each([
    [30, "a few seconds"],
    [3 * 24 * 60 * 60 * 1000, "72h"],
    [2 * 24 * 60 * 60 * 1000 + 3 * 60 * 60 * 1000, "51h"],
    [24 * 60 * 60 * 1000 + 4 * 60 * 1000, "24h 4m"],
  ])("should format time elapsed (%i) correctly", async (elapsedTime, expected) => {
    const result = await render(
      <LatestSyncCell
        status={ConnectionStatusIndicatorStatus.Syncing}
        recordsLoaded={3000}
        recordsExtracted={5000}
        syncStartedAt={BASE_TIME - elapsedTime}
        isLoadingHistoricalData={false}
      />
    );

    expect(result.container.textContent).toBe(`3,000 loaded | ${expected} elapsed`);
  });
});
