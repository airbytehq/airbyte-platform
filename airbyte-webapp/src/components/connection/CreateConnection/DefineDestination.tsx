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

import { useDestinationList } from "core/api";
import { DestinationRead, DestinationReadList } from "core/api/types/AirbyteClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import { BackToDefineSourceButton } from "./BackToDefineSourceButton";
import { CreateNewDestination, DESTINATION_DEFINITION_PARAM } from "./CreateNewDestination";
import { RadioButtonTiles } from "./RadioButtonTiles";
import { SelectExistingConnector } from "./SelectExistingConnector";

export const EXISTING_DESTINATION_TYPE = "existing";
export const NEW_DESTINATION_TYPE = "new";
const VALID_VIEWS = [EXISTING_DESTINATION_TYPE, NEW_DESTINATION_TYPE] as const;
export type View = (typeof VALID_VIEWS)[number];
export const DESTINATION_TYPE_PARAM = "destinationType";
export const DESTINATION_ID_PARAM = "destinationId";

const PAGE_SIZE = 10;

export const DefineDestination: React.FC = () => {
  const [searchParams] = useSearchParams();
  const searchParam = searchParams.get(DESTINATION_TYPE_PARAM) as View;
  const viewFromSearchParam = VALID_VIEWS.includes(searchParam) ? searchParam : null;
  // This query is used to check if there are any existing sources so we can render the correct initial view. It also
  // serves to warm the cache for the query inside DefineSourceView, which will be mutated as the user searches and paginates.
  const existingDestinationsQuery = useDestinationList({ pageSize: PAGE_SIZE, filters: { searchTerm: "" } });

  const pages = existingDestinationsQuery.data?.pages;
  const hasExistingDestinations = pages?.[0]?.destinations.length !== 0;

  const initialView = viewFromSearchParam === "new" ? "new" : hasExistingDestinations ? "existing" : "new";

  if (existingDestinationsQuery.isLoading) {
    return <LoadingPage />;
  }

  return <DefineDestinationView initialView={initialView} />;
};

const DefineDestinationView: React.FC<{ initialView: View }> = ({ initialView }) => {
  useTrackPage(PageTrackingCodes.CONNECTIONS_NEW_DEFINE_DESTINATION);
  const [selectedView, setSelectedView] = useState<View>(initialView);
  const [searchParams, setSearchParams] = useSearchParams();
  const { formatMessage } = useIntl();
  const [searchTerm, setSearchTerm] = useState("");
  const existingDestinationsQuery = useDestinationList({ pageSize: PAGE_SIZE, filters: { searchTerm } });

  const infiniteDestinations = useMemo<DestinationReadList>(
    () => ({
      destinations: existingDestinationsQuery.data?.pages?.flatMap<DestinationRead>((page) => page.destinations) ?? [],
    }),
    [existingDestinationsQuery.data]
  );

  const selectDestinationType = (destinationType: View) => {
    searchParams.delete(DESTINATION_DEFINITION_PARAM);
    searchParams.set(DESTINATION_TYPE_PARAM, destinationType);
    setSearchParams(searchParams);
    setSelectedView(destinationType);
  };

  const selectDestination = (destinationId: string) => {
    searchParams.delete(DESTINATION_TYPE_PARAM);
    searchParams.set(DESTINATION_ID_PARAM, destinationId);
    setSearchParams(searchParams);
  };

  const hasExistingDestinations = existingDestinationsQuery.isSuccess && infiniteDestinations.destinations.length !== 0;

  return (
    <PageContainer centered>
      <Box p="xl">
        <FlexContainer direction="column">
          {!searchParams.get(DESTINATION_DEFINITION_PARAM) && (
            <PageContainer centered>
              <Card>
                <Heading as="h2">
                  <FormattedMessage id="connectionForm.defineDestination" />
                </Heading>
                <Box mt="md">
                  <RadioButtonTiles
                    name="destinationType"
                    options={[
                      {
                        value: EXISTING_DESTINATION_TYPE,
                        label: formatMessage({ id: "connectionForm.destinationExisting" }),
                        description: formatMessage({ id: "connectionForm.destinationExistingDescription" }),
                        disabled: !hasExistingDestinations && searchTerm.length === 0,
                      },
                      {
                        value: NEW_DESTINATION_TYPE,
                        label: formatMessage({ id: "connectionForm.destinationNew" }),
                        description: formatMessage({ id: "connectionForm.destinationNewDescription" }),
                      },
                    ]}
                    selectedValue={selectedView}
                    onSelectRadioButton={(id) => selectDestinationType(id)}
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
              {!existingDestinationsQuery.isLoading && infiniteDestinations.destinations.length > 0 && (
                <SelectExistingConnector
                  connectors={infiniteDestinations.destinations}
                  selectConnector={selectDestination}
                  hasNextPage={!!existingDestinationsQuery.hasNextPage}
                  fetchNextPage={() => existingDestinationsQuery.fetchNextPage()}
                  isFetchingNextPage={existingDestinationsQuery.isFetchingNextPage}
                />
              )}
              {existingDestinationsQuery.isLoading && (
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
              {!existingDestinationsQuery.isLoading &&
                searchTerm.length > 0 &&
                infiniteDestinations.destinations.length === 0 && (
                  <Box mt="xl">
                    <FlexContainer justifyContent="center" gap="md">
                      <Text>
                        <FormattedMessage id="tables.destinations.filters.empty" />
                      </Text>
                    </FlexContainer>
                  </Box>
                )}
            </PageContainer>
          )}
          {selectedView === "new" && <CreateNewDestination />}
          {selectedView !== "new" && <BackToDefineSourceButton />}
          <CloudInviteUsersHint connectorType="destination" />
        </FlexContainer>
      </Box>
    </PageContainer>
  );
};
