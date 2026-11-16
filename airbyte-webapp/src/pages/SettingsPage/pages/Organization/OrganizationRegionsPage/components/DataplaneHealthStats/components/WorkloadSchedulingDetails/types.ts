import { AgeMessage } from "../../types";

export type BlockedWorkloads =
  | { kind: "none" }
  // Only the longest blocked workload is named, with the number of blocked workloads in all.
  | { kind: "blocked"; count: number; longest: { workloadType: string; waiting: AgeMessage } }
  | { kind: "notMeasured" };

export interface ScheduledWorkload {
  workloadType: string;
  age: AgeMessage;
}

export interface SchedulingProblem {
  message: string;
  age: AgeMessage;
}

export interface WorkloadSchedulingDetailsData {
  blocked: BlockedWorkloads;
  lastScheduled?: ScheduledWorkload;
  lastProblem?: SchedulingProblem;
  // Present when the check failed or could not complete.
  error?: { code: string; message: string };
}
