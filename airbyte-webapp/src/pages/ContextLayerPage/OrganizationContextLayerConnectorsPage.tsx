import React from "react";

import { ContextLayerConnectorsPage } from "cloud/components/AgentsOptIn";

const OrganizationContextLayerConnectorsPage: React.FC<{ actorKind: "source" | "destination" }> = ({ actorKind }) => (
  <ContextLayerConnectorsPage actorKind={actorKind} />
);

export default OrganizationContextLayerConnectorsPage;
