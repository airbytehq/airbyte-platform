import { mockDestinationDefinition } from "test-utils/mock-data/mockDestination";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";

import { JobType } from "core/api/types/AirbyteClient";

import { getResourceOptions } from "./ResourceAllocationMenu";

describe("#getOptions", () => {
  it("uses values from actor definition, if present, for source connector", () => {
    const sourceDefinition = {
      ...mockSourceDefinition,
      resourceRequirements: {
        jobSpecific: [
          {
            jobType: JobType.sync,
            resourceRequirements: { memory_request: "9", cpu_request: "7" },
          },
        ],
      },
    };
    const options = getResourceOptions(sourceDefinition);
    expect(options[0].value).toEqual({ memory: "9", cpu: "7" });
  });

  it("uses values from hardcoded defaults, if no actor value present, for source connector", () => {
    const options = getResourceOptions(mockSourceDefinition);
    expect(options[0].value).toEqual({ memory: "2", cpu: "2" });
  });

  it("uses values from actor definition, if present, for destination connector", () => {
    const destinationDefinition = {
      ...mockDestinationDefinition,
      resourceRequirements: {
        jobSpecific: [
          {
            jobType: JobType.sync,
            resourceRequirements: { memory_request: "9", cpu_request: "7" },
          },
        ],
      },
    };
    const options = getResourceOptions(destinationDefinition);
    expect(options[0].value).toEqual({ memory: "9", cpu: "7" });
  });
  it("uses values from hardcoded defaults, if no actor value present, for destination connector", () => {
    const options = getResourceOptions(mockDestinationDefinition);
    expect(options[0].value).toEqual({ memory: "2", cpu: "2" });
  });
});
