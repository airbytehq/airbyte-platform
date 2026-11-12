import { AgeMessage, CardData } from "../../types";

export interface SecretStorageCardData extends CardData {
  // Absent when there is no check to date, such as for a data plane that never connected.
  lastChecked?: AgeMessage;
}
