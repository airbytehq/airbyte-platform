import { StreamStatusType } from "components/connection/StreamStatusIndicator";
import { render } from "test-utils";

import { LatestSyncCell } from "./LatestSyncCell";

const BASE_TIME = 1719860400000;

jest.useFakeTimers().setSystemTime(BASE_TIME);

describe("LastSyncCell", () => {
  it("past sync", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Synced}
        recordsLoaded={1000}
        recordsExtracted={1000}
        bytesLoaded={1024 * 1024 * 1024}
        bytesExtracted={1024 * 1024 * 1024}
        syncStartedAt={BASE_TIME - 1000}
        isLoadingHistoricalData={false}
        showBytes={false}
      />
    );

    expect(result.container.textContent).toBe("1,000 loaded");
  });
  it("past sync showing bytes", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Synced}
        recordsLoaded={1000}
        recordsExtracted={1000}
        bytesLoaded={1024 * 1024 * 1024}
        bytesExtracted={1024 * 1024 * 1024}
        syncStartedAt={BASE_TIME - 1000}
        isLoadingHistoricalData={false}
        showBytes
      />
    );

    expect(result.container.textContent).toBe("1 GB loaded");
  });

  it("past sync without recordsLoaded", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Synced}
        recordsLoaded={undefined}
        recordsExtracted={undefined}
        bytesLoaded={undefined}
        bytesExtracted={undefined}
        syncStartedAt={BASE_TIME - 1000}
        isLoadingHistoricalData={false}
        showBytes={false}
      />
    );

    expect(result.container.textContent).toBe("-");
  });
  it("past sync with nothing loaded but showing bytes", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Synced}
        recordsLoaded={undefined}
        recordsExtracted={undefined}
        bytesLoaded={undefined}
        bytesExtracted={undefined}
        syncStartedAt={BASE_TIME - 1000}
        isLoadingHistoricalData={false}
        showBytes
      />
    );

    expect(result.container.textContent).toBe("-");
  });

  it("extracted == 0 && loaded == 0, show bytes false", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={0}
        recordsExtracted={0}
        bytesLoaded={0}
        bytesExtracted={0}
        syncStartedAt={BASE_TIME - 100_000}
        isLoadingHistoricalData={false}
        showBytes={false}
      />
    );

    expect(result.container.textContent).toBe("Starting… | 1m elapsed");
  });
  it("extracted == 0 && loaded == 0, show bytes true", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={0}
        recordsExtracted={0}
        bytesLoaded={0}
        bytesExtracted={0}
        syncStartedAt={BASE_TIME - 100_000}
        isLoadingHistoricalData={false}
        showBytes
      />
    );

    expect(result.container.textContent).toBe("Starting… | 1m elapsed");
  });

  it("extracted > 0 && loaded == 0", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={0}
        recordsExtracted={5000}
        bytesLoaded={0}
        bytesExtracted={1024 * 1024 * 200} // 200MB
        syncStartedAt={BASE_TIME - 130_000}
        isLoadingHistoricalData={false}
        showBytes={false}
      />
    );

    expect(result.container.textContent).toBe("5,000 extracted | 2m elapsed");
  });
  it("extracted > 0 && loaded == 0 and show bytes", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={0}
        recordsExtracted={5000}
        bytesLoaded={0}
        bytesExtracted={1024 * 1024 * 200} // 200MB
        syncStartedAt={BASE_TIME - 130_000}
        isLoadingHistoricalData={false}
        showBytes
      />
    );

    expect(result.container.textContent).toBe("200 MB extracted | 2m elapsed");
  });

  it("records extracted > 0 && loaded > 0", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={3000}
        recordsExtracted={5000}
        bytesLoaded={1024 * 1024 * 200} // 200MB
        bytesExtracted={1024 * 1024 * 500} // 500MB
        syncStartedAt={BASE_TIME - 130_000}
        isLoadingHistoricalData={false}
        showBytes={false}
      />
    );

    expect(result.container.textContent).toBe("3,000 loaded | 2m elapsed");
  });

  it("bytes extracted > 0 && loaded > 0", async () => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={3000}
        recordsExtracted={5000}
        bytesLoaded={1024 * 1024 * 200} // 200MB
        bytesExtracted={1024 * 1024 * 500} // 500MB
        syncStartedAt={BASE_TIME - 130_000}
        isLoadingHistoricalData={false}
        showBytes
      />
    );

    expect(result.container.textContent).toBe("200 MB loaded | 2m elapsed");
  });

  it.each([
    [30, "a few seconds"],
    [3 * 24 * 60 * 60 * 1000, "72h"],
    [2 * 24 * 60 * 60 * 1000 + 3 * 60 * 60 * 1000, "51h"],
    [24 * 60 * 60 * 1000 + 4 * 60 * 1000, "24h 4m"],
  ])("should format time elapsed (%i) correctly", async (elapsedTime, expected) => {
    const result = await render(
      <LatestSyncCell
        status={StreamStatusType.Syncing}
        recordsLoaded={3000}
        recordsExtracted={5000}
        syncStartedAt={BASE_TIME - elapsedTime}
        isLoadingHistoricalData={false}
        showBytes={false}
      />
    );

    expect(result.container.textContent).toBe(`3,000 loaded | ${expected} elapsed`);
  });
});
