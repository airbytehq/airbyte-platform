import { DiagnosticKey } from "./types";

export const SECRET_STORAGE_KEY = "secretStorage" satisfies DiagnosticKey;
export const WORKLOAD_SCHEDULING_KEY = "workloadScheduling" satisfies DiagnosticKey;

// The launcher probes each secret store at this rate (workload-launcher.secret-storage-probe.rate: PT30M).
export const SECRET_STORAGE_CHECK_INTERVAL_MINUTES = 30;
