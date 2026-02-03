import { useMemo } from "react";

import { SchemaChange } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";

export const useSchemaChanges = (schemaChange: SchemaChange) => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  return useMemo(() => {
    const hasSchemaChanges = allowAutoDetectSchema && schemaChange !== SchemaChange.no_change;
    const hasBreakingSchemaChange = hasSchemaChanges && schemaChange === SchemaChange.breaking;
    const hasNonBreakingSchemaChange = hasSchemaChanges && schemaChange === SchemaChange.non_breaking;

    return {
      schemaChange,
      hasSchemaChanges,
      hasBreakingSchemaChange,
      hasNonBreakingSchemaChange,
    };
  }, [allowAutoDetectSchema, schemaChange]);
};
