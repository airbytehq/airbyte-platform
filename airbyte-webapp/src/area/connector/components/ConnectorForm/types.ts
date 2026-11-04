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

/** The host and port that the Connect command tests instead of a connector check. */
export interface ConnectTarget {
  host: string;
  port: number;
}

/** Checks one section of the form. With a target, the check uses the Connect command instead of a connector check. */
export type CheckSection = (values: ConnectorFormValues, connectTarget?: ConnectTarget) => Promise<void>;

// The whole ConnectorCard form values
export type ConnectorCardValues = { serviceType: string } & ConnectorFormValues;
