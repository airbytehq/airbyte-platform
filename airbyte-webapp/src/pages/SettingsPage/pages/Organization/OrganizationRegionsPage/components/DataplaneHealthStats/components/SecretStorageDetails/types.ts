import { AgeMessage } from "../../types";
import { StatusLevel } from "../StatusCard";

export type StoreScopeType = "workspace" | "organization";

export interface StoreScope {
  type: StoreScopeType;
  id: string;
}

export interface SecretStorageRow {
  id: string;
  name?: string;
  level: StatusLevel;
  scope?: StoreScope;
  lastChecked: AgeMessage;
  // Present when the check failed or could not complete.
  error?: { code: string; message: string };
}
