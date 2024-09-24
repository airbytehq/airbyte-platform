import { impactPriority } from "./constants";
import { ActiveIncident, IncidentStatus, PageStatus } from "./types";

export const findHighestImpactIncident = (incidents: ActiveIncident[]) => {
  return incidents.reduce(
    (highest, current) => {
      if (!highest) {
        return current;
      }
      const currentPriority = impactPriority[current.impact] || 0;
      const highestPriority = impactPriority[highest.impact] || 0;
      return currentPriority > highestPriority ? current : highest;
    },
    undefined as ActiveIncident | undefined
  );
};

export interface GetCloudStatusResponse {
  page: { name: string; url: string; status: PageStatus };
  activeIncidents?: ActiveIncident[];
  activeMaintenances?: Array<{ name: string; start: string; status: IncidentStatus; duration: number; url: string }>;
}

export const getCloudStatus = async (): Promise<GetCloudStatusResponse | undefined> => {
  const response = await fetch("https://status.airbyte.com/summary.json");
  if (response.ok) {
    return await response.json();
  }
  return undefined;
};
