import { useMemo } from "react";
import { z } from "zod";

import { useCurrentWorkspace, useListDataplaneGroups } from "core/api";
import { useExperiment } from "core/services/Experiment";
import { MAX_ARRAY_LENGTH } from "core/utils/zod";

const airbyteCloudIpAddressesByDataplanesSchema = z.record(z.string(), z.array(z.string()).max(MAX_ARRAY_LENGTH));

export const useAirbyteCloudIpsByDataplane = (): string[] => {
  const { dataplaneGroupId } = useCurrentWorkspace();
  const dataplaneGroups = useListDataplaneGroups();

  const dataplaneGroup = useMemo(
    () => dataplaneGroups.find((group) => group.dataplane_group_id === dataplaneGroupId),
    [dataplaneGroups, dataplaneGroupId]
  );

  const dataplaneGroupName = dataplaneGroup?.name?.toLowerCase();

  const experimentValue = useExperiment("connector.airbyteCloudIpAddressesByDataplane");
  const parseResult = airbyteCloudIpAddressesByDataplanesSchema.safeParse(experimentValue);

  if (!parseResult.success) {
    throw new Error(`Invalid experiment value for connector.airbyteCloudIpAddressesByDataplane`);
  }

  const ipAddressesByDataplanes = parseResult.data;
  const dataplaneIps = ipAddressesByDataplanes[dataplaneGroupName ?? "auto"];

  if (!dataplaneIps) {
    // if no dataplane group is found, try to return the default IPs(not splitted on dataplane groups)
    return ipAddressesByDataplanes.auto;
  }

  return dataplaneIps;
};
