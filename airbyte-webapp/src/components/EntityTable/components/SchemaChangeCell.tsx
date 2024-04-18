import React from "react";

import { Link } from "components/ui/Link";

import { ConnectionId, SchemaChange } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { ConnectionRoutePaths } from "pages/routePaths";

import { ChangesStatusIcon } from "./ChangesStatusIcon";

interface SchemaChangeCellProps {
  connectionId: ConnectionId;
  schemaChange: SchemaChange;
}

export const SchemaChangeCell: React.FC<SchemaChangeCellProps> = ({ connectionId, schemaChange }) => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  if (!allowAutoDetectSchema || schemaChange !== SchemaChange.breaking) {
    return null;
  }

  return (
    <Link to={`${connectionId}/${ConnectionRoutePaths.Replication}`} data-testid={`link-replication-${connectionId}`}>
      <ChangesStatusIcon schemaChange={schemaChange} />
    </Link>
  );
};
