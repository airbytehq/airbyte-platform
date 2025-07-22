import { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { CloudInviteUsersHint } from "components/CloudInviteUsersHint";
import LoadingPage from "components/LoadingPage";
import { PageContainer } from "components/PageContainer";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { useSourceList } from "core/api";
import { SourceRead, SourceReadList } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import { CreateNewSource, SOURCE_DEFINITION_PARAM } from "./CreateNewSource";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

export const EXISTING_SOURCE_TYPE = "existing";
export const NEW_SOURCE_TYPE = "new";
const VALID_VIEWS = [EXISTING_SOURCE_TYPE, NEW_SOURCE_TYPE] as const;
export type View = (typeof VALID_VIEWS)[number];
export const SOURCE_TYPE_PARAM = "sourceType";
export const SOURCE_ID_PARAM = "sourceId";

const PAGE_SIZE = 10;

export const DefineSource: React.FC = () => {
  const [searchParams] = useSearchParams();
  const searchParam = searchParams.get(SOURCE_TYPE_PARAM) as View;
  const viewFromSearchParam = VALID_VIEWS.includes(searchParam) ? searchParam : null;
  // This query is used to check if there are any existing sources so we can render the correct initial view. It also
  // serves to warm the cache for the query inside DefineSourceView, which will be mutated as the user searches and paginates.
  const existingSourcesQuery = useSourceList({ pageSize: PAGE_SIZE, filters: { searchTerm: "" } });

  const pages = existingSourcesQuery.data?.pages;
  const hasExistingSources = pages?.[0]?.sources.length !== 0;

  const initialView = viewFromSearchParam === "new" ? "new" : hasExistingSources ? "existing" : "new";

  if (existingSourcesQuery.isLoading) {
    return <LoadingPage />;
  }

  return <DefineSourceView initialView={initialView} />;
};

const DefineSourceView: React.FC<{ initialView: "new" | "existing" }> = ({ initialView }) => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_DEFINE_SOURCE);
  const [selectedView, setSelectedView] = useState<View>(initialView);
  const [searchParams, setSearchParams] = useSearchParams();
  const { formatMessage } = useIntl();
  const [searchTerm, setSearchTerm] = useState("");
  const existingSourcesQuery = useSourceList({ pageSize: PAGE_SIZE, filters: { searchTerm } });

  const infiniteSources = useMemo<SourceReadList>(
    () => ({
      sources: existingSourcesQuery.data?.pages?.flatMap<SourceRead>((page) => page.sources) ?? [],
    }),
    [existingSourcesQuery.data]
  );

  const selectSourceType = (sourceType: View) => {
    searchParams.delete(SOURCE_DEFINITION_PARAM);
    searchParams.set(SOURCE_TYPE_PARAM, sourceType);
    setSearchParams(searchParams);
    setSelectedView(sourceType);
  };

  const selectSource = (sourceId: string) => {
    searchParams.delete(SOURCE_TYPE_PARAM);
    searchParams.set(SOURCE_ID_PARAM, sourceId);
    setSearchParams(searchParams);
  };

  const hasExistingSources = existingSourcesQuery.isSuccess && infiniteSources.sources.length !== 0;

  return (
    <Box p="xl">
      <FlexContainer direction="column">
        {!searchParams.get(SOURCE_DEFINITION_PARAM) && (
          <PageContainer centered>
            <Card>
              <Heading as="h2">
                <FormattedMessage id="connectionForm.defineSource" />
              </Heading>
              <Box mt="md">
                <RadioButtonTiles
                  name="sourceType"
                  options={[
                    {
                      value: EXISTING_SOURCE_TYPE,
                      label: formatMessage({ id: "connectionForm.sourceExisting" }),
                      description: formatMessage({ id: "connectionForm.sourceExistingDescription" }),
                      disabled: !hasExistingSources && searchTerm.length === 0,
                    },
                    {
                      value: NEW_SOURCE_TYPE,
                      label: formatMessage({ id: "onboarding.sourceSetUp" }),
                      description: formatMessage({ id: "onboarding.sourceSetUp.description" }),
                    },
                  ]}
                  selectedValue={selectedView}
                  onSelectRadioButton={(id) => selectSourceType(id)}
                />
              </Box>
            </Card>
          </PageContainer>
        )}
        {selectedView === "existing" && (
          <PageContainer centered>
            <Box mb="md">
              <SearchInput value={searchTerm} onChange={setSearchTerm} debounceTimeout={300} />
            </Box>
            {!existingSourcesQuery.isLoading && infiniteSources.sources.length > 0 && (
              <SelectExistingConnector
                connectors={infiniteSources.sources}
                selectConnector={selectSource}
                hasNextPage={!!existingSourcesQuery.hasNextPage}
                fetchNextPage={() => existingSourcesQuery.fetchNextPage()}
                isFetchingNextPage={existingSourcesQuery.isFetchingNextPage}
              />
            )}
            {existingSourcesQuery.isLoading && (
              <Card>
                <FlexContainer direction="column" gap="lg">
                  <LoadingSkeleton />
                  <LoadingSkeleton />
                  <LoadingSkeleton />
                  <LoadingSkeleton />
                  <LoadingSkeleton />
                </FlexContainer>
              </Card>
            )}
            {!existingSourcesQuery.isLoading && searchTerm.length > 0 && infiniteSources.sources.length === 0 && (
              <Box mt="xl">
                <FlexContainer justifyContent="center" gap="md">
                  <Text>
                    <FormattedMessage id="tables.sources.filters.empty" />
                  </Text>
                </FlexContainer>
              </Box>
            )}
          </PageContainer>
        )}
        {selectedView === "new" && <CreateNewSource />}
        <CloudInviteUsersHint connectorType="source" />
      </FlexContainer>
    </Box>
  );
};
