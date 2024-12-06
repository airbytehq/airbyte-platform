import { PageContainer } from "components/PageContainer";
import { ScrollParent } from "components/ui/ScrollParent";

import { FeatureItem, IfFeatureDisabled, IfFeatureEnabled } from "core/services/features";

import { MappingsEmptyState } from "./MappingsEmptyState";
import { MappingsUpsellEmptyState } from "./MappingsUpsellEmptyState";

export const ConnectionMappingsPage = () => {
  const existingMappings = [];

  return (
    <ScrollParent>
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
    </ScrollParent>
  );
};
