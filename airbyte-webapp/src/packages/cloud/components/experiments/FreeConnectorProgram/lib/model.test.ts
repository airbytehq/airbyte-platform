import { mockConnection } from "test-utils";
import { mockDestinationDefinition } from "test-utils/mock-data/mockDestination";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";

import { DestinationDefinitionRead, SourceDefinitionRead, WebBackendConnectionRead } from "core/request/AirbyteClient";

import {
  isConnectionEligibleForFCP,
  isDestinationDefinitionEligibleForFCP,
  isSourceDefinitionEligibleForFCP,
} from "./model";

const deprecatedConnection: WebBackendConnectionRead = { ...mockConnection, status: "deprecated" };
const alphaSource: SourceDefinitionRead = { ...mockSourceDefinition, releaseStage: "alpha" };
const betaSource: SourceDefinitionRead = { ...mockSourceDefinition, releaseStage: "beta" };
const gaSource: SourceDefinitionRead = {
  ...mockSourceDefinition,
  releaseStage: "generally_available",
};
const customSource: SourceDefinitionRead = { ...mockSourceDefinition, releaseStage: "custom" };
const alphaDestination: DestinationDefinitionRead = { ...mockDestinationDefinition, releaseStage: "alpha" };
const betaDestination: DestinationDefinitionRead = { ...mockDestinationDefinition, releaseStage: "beta" };
const gaDestination: DestinationDefinitionRead = {
  ...mockDestinationDefinition,
  releaseStage: "generally_available",
};
const customDestination: DestinationDefinitionRead = { ...mockDestinationDefinition, releaseStage: "custom" };

describe(`${isConnectionEligibleForFCP.name}`, () => {
  it("returns true for an alpha source and GA destination", () => {
    expect(isConnectionEligibleForFCP(mockConnection, alphaSource, gaDestination)).toBe(true);
  });
  it("returns true for a GA source and alpha destination", () => {
    expect(isConnectionEligibleForFCP(mockConnection, gaSource, alphaDestination)).toBe(true);
  });
  it("returns false for a deprecated connection", () => {
    expect(isConnectionEligibleForFCP(deprecatedConnection, alphaSource, alphaDestination)).toBe(false);
  });
  it("returns false for a custom source", () => {
    expect(isConnectionEligibleForFCP(deprecatedConnection, customSource, alphaDestination)).toBe(false);
  });
});

describe(`${isSourceDefinitionEligibleForFCP.name}`, () => {
  it("returns true for an alpha destination", () => {
    expect(isSourceDefinitionEligibleForFCP(alphaSource)).toBe(true);
  });
  it("returns true for an beta destination", () => {
    expect(isSourceDefinitionEligibleForFCP(betaSource)).toBe(true);
  });
  it("returns false for an GA destination", () => {
    expect(isSourceDefinitionEligibleForFCP(gaSource)).toBe(false);
  });
  it("returns false for a custom destination", () => {
    expect(isSourceDefinitionEligibleForFCP(customSource)).toBe(false);
  });
});

describe(`${isDestinationDefinitionEligibleForFCP.name}`, () => {
  it("returns true for an alpha destination", () => {
    expect(isDestinationDefinitionEligibleForFCP(alphaDestination)).toBe(true);
  });
  it("returns true for an beta destination", () => {
    expect(isDestinationDefinitionEligibleForFCP(betaDestination)).toBe(true);
  });
  it("returns false for an GA destination", () => {
    expect(isDestinationDefinitionEligibleForFCP(gaDestination)).toBe(false);
  });
  it("returns false for a custom destination", () => {
    expect(isDestinationDefinitionEligibleForFCP(customDestination)).toBe(false);
  });
});
