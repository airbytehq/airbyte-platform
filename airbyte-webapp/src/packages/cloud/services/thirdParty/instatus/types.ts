import { INCIDENT_STATUS, PAGE_STATUS } from "./constants";

export type PageStatus = (typeof PAGE_STATUS)[keyof typeof PAGE_STATUS];
export type IncidentStatus = "INVESTIGATING" | "HASISSUES" | "UNDERMAINTENANCE";
export type Impact = (typeof INCIDENT_STATUS)[keyof typeof INCIDENT_STATUS];

export interface ActiveIncident {
  name: string;
  started: string;
  status: IncidentStatus;
  impact: Impact;
  url: string;
}
