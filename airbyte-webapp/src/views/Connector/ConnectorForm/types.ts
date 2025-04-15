// TODO: This needs to be converted to interface, but has int he current state a problem with index signatures

import {
  DestinationConfiguration,
  ScopedResourceRequirements,
  SourceConfiguration,
} from "core/api/types/AirbyteClient";

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export type ConnectorFormValues = {
  name: string;
  connectionConfiguration: SourceConfiguration | DestinationConfiguration;
  resourceAllocation: ScopedResourceRequirements;
};

// The whole ConnectorCard form values
export type ConnectorCardValues = { serviceType: string } & ConnectorFormValues;
