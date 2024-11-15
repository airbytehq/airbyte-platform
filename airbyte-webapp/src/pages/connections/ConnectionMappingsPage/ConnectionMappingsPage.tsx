import { PageContainer } from "components/PageContainer";
import { ScrollableContainer } from "components/ScrollableContainer";

import { FeatureItem, IfFeatureDisabled, IfFeatureEnabled } from "core/services/features";

import { MappingsEmptyState } from "./MappingsEmptyState";
import { MappingsUpsellEmptyState } from "./MappingsUpsellEmptyState";

export const ConnectionMappingsPage = () => {
  const existingMappings = [];

  return (
    <ScrollableContainer>
      <PageContainer centered>
        {existingMappings.length === 0 && (
          <>
            <IfFeatureDisabled feature={FeatureItem.MappingsUI}>
              <MappingsUpsellEmptyState />
            </IfFeatureDisabled>
            <IfFeatureEnabled feature={FeatureItem.MappingsUI}>
              <MappingsEmptyState />
            </IfFeatureEnabled>
          </>
        )}
      </PageContainer>
    </ScrollableContainer>
  );
};
