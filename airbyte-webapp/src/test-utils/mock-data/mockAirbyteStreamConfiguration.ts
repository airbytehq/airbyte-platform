import { AirbyteStreamConfiguration } from "core/api/types/AirbyteClient";

export const mockStreamConfiguration: AirbyteStreamConfiguration = {
  fieldSelectionEnabled: false,
  selectedFields: [],
  selected: true,
  syncMode: "full_refresh",
  destinationSyncMode: "overwrite",
};
