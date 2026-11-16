import { AgeMessage, CardData } from "../../types";

// The checks that have details to open. Only one set of details is open at a time.
export type DiagnosticKey = "secretStorage" | "workloadScheduling";

export interface DiagnosticCardData extends CardData {
  // Absent when there is no check to date, such as for a data plane that never connected.
  lastChecked?: AgeMessage;
}
