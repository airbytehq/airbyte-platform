import { renderHook, act } from "@testing-library/react";

import { useGetDataWorkerAvailability } from "core/api";
import { useConfirmationModalService } from "core/services/ConfirmationModal";
import { useFeature } from "core/services/features/FeatureService";
import { trackError } from "core/utils/datadog";

import { useSyncWithCapacityCheck } from "./useSyncWithCapacityCheck";

jest.mock("core/api", () => ({
  useGetDataWorkerAvailability: jest.fn(),
}));

jest.mock("core/services/ConfirmationModal", () => ({
  useConfirmationModalService: jest.fn(),
}));

jest.mock("core/services/features/FeatureService", () => ({
  useFeature: jest.fn(),
}));

jest.mock("core/utils/datadog", () => ({
  trackError: jest.fn(),
}));

const mockUseFeature = jest.mocked(useFeature);
const mockUseGetDataWorkerAvailability = jest.mocked(useGetDataWorkerAvailability);
const mockUseConfirmationModalService = jest.mocked(useConfirmationModalService);
const mockTrackError = jest.mocked(trackError);

describe("useSyncWithCapacityCheck", () => {
  const mockSyncConnection = jest.fn().mockResolvedValue(undefined);
  const mockGetDataWorkerAvailability = jest.fn();
  const mockOpenConfirmationModal = jest.fn();
  const mockCloseConfirmationModal = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
    mockUseGetDataWorkerAvailability.mockReturnValue(mockGetDataWorkerAvailability);
    mockUseConfirmationModalService.mockReturnValue({
      openConfirmationModal: mockOpenConfirmationModal,
      closeConfirmationModal: mockCloseConfirmationModal,
    });
  });

  it("calls syncConnection directly when feature flag is disabled", async () => {
    mockUseFeature.mockReturnValue(false);

    const { result } = renderHook(() => useSyncWithCapacityCheck(mockSyncConnection));

    await act(async () => {
      await result.current.syncWithCapacityCheck();
    });

    expect(mockGetDataWorkerAvailability).not.toHaveBeenCalled();
    expect(mockSyncConnection).toHaveBeenCalledTimes(1);
    expect(mockOpenConfirmationModal).not.toHaveBeenCalled();
  });

  it("calls syncConnection directly when capacity is available", async () => {
    mockUseFeature.mockReturnValue(true);
    mockGetDataWorkerAvailability.mockResolvedValue({ outOfDataWorkers: false });

    const { result } = renderHook(() => useSyncWithCapacityCheck(mockSyncConnection));

    await act(async () => {
      await result.current.syncWithCapacityCheck();
    });

    expect(mockGetDataWorkerAvailability).toHaveBeenCalledTimes(1);
    expect(mockSyncConnection).toHaveBeenCalledTimes(1);
    expect(mockOpenConfirmationModal).not.toHaveBeenCalled();
  });

  it("opens confirmation modal when capacity is exhausted", async () => {
    mockUseFeature.mockReturnValue(true);
    mockGetDataWorkerAvailability.mockResolvedValue({ outOfDataWorkers: true });

    const { result } = renderHook(() => useSyncWithCapacityCheck(mockSyncConnection));

    await act(async () => {
      await result.current.syncWithCapacityCheck();
    });

    expect(mockGetDataWorkerAvailability).toHaveBeenCalledTimes(1);
    expect(mockSyncConnection).not.toHaveBeenCalled();
    expect(mockOpenConfirmationModal).toHaveBeenCalledTimes(1);
    expect(mockOpenConfirmationModal).toHaveBeenCalledWith(
      expect.objectContaining({
        title: "connection.sync.capacityWarning.title",
        text: "connection.sync.capacityWarning.body",
        submitButtonText: "connection.sync.capacityWarning.submit",
        submitButtonVariant: "primary",
      })
    );
  });

  it("calls syncConnection and closes modal when user clicks Queue sync", async () => {
    mockUseFeature.mockReturnValue(true);
    mockGetDataWorkerAvailability.mockResolvedValue({ outOfDataWorkers: true });

    const { result } = renderHook(() => useSyncWithCapacityCheck(mockSyncConnection));

    await act(async () => {
      await result.current.syncWithCapacityCheck();
    });

    const modalOptions = mockOpenConfirmationModal.mock.calls[0][0];

    await act(async () => {
      await modalOptions.onSubmit();
    });

    expect(mockSyncConnection).toHaveBeenCalledTimes(1);
    expect(mockCloseConfirmationModal).toHaveBeenCalledTimes(1);
  });

  it("falls back to syncConnection when capacity check fails", async () => {
    mockUseFeature.mockReturnValue(true);
    const error = new Error("API failure");
    mockGetDataWorkerAvailability.mockRejectedValue(error);

    const { result } = renderHook(() => useSyncWithCapacityCheck(mockSyncConnection));

    await act(async () => {
      await result.current.syncWithCapacityCheck();
    });

    expect(mockTrackError).toHaveBeenCalledWith(error, { context: "data_worker_capacity_check" });
    expect(mockSyncConnection).toHaveBeenCalledTimes(1);
    expect(mockOpenConfirmationModal).not.toHaveBeenCalled();
  });

  it("sets isCheckingCapacity during capacity check", async () => {
    mockUseFeature.mockReturnValue(true);

    let resolveCapacity: (value: { outOfDataWorkers: boolean }) => void;
    mockGetDataWorkerAvailability.mockReturnValue(
      new Promise((resolve) => {
        resolveCapacity = resolve;
      })
    );

    const { result } = renderHook(() => useSyncWithCapacityCheck(mockSyncConnection));

    expect(result.current.isCheckingCapacity).toBe(false);

    let syncPromise: Promise<void>;
    act(() => {
      syncPromise = result.current.syncWithCapacityCheck();
    });

    expect(result.current.isCheckingCapacity).toBe(true);

    await act(async () => {
      resolveCapacity!({ outOfDataWorkers: false });
      await syncPromise;
    });

    expect(result.current.isCheckingCapacity).toBe(false);
  });
});
