import { zodResolver } from "@hookform/resolvers/zod";
import { useState } from "react";
import { FormProvider, useForm, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useNavigate, useSearchParams } from "react-router-dom";

import { SOURCE_ID_PARAM } from "components/connection/CreateConnection/DefineSource";
import { CreateConnectionFlowLayout } from "components/connection/CreateConnectionFlowLayout";
import LoadingSchema from "components/LoadingSchema";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink, Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useGetDestinationFromSearchParams, useGetSourceFromSearchParams } from "area/connector/utils";
import { StreamMappings } from "area/dataActivation/components/ConnectionForm/StreamMappings";
import { DataActivationConnectionFormValues } from "area/dataActivation/types";
import { DataActivationConnectionFormSchema, EMPTY_STREAM } from "area/dataActivation/utils";
import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useDestinationDefinitionList, useDiscoverDestination, useDiscoverSchemaQuery } from "core/api";
import { links } from "core/utils/links";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./DataActivationMappingPage.module.scss";
import { useStreamMappings } from "../CreateDataActivationConnectionRoutes";

export const DataActivationMappingPage: React.FC = () => {
  const [searchParams] = useSearchParams();
  const { streamMappings, setStreamMappings } = useStreamMappings();
  const source = useGetSourceFromSearchParams();
  const destination = useGetDestinationFromSearchParams();
  const createLink = useCurrentWorkspaceLink();
  const { data: discoveredSource } = useDiscoverSchemaQuery(source.sourceId);
  const { data: discoveredDestination } = useDiscoverDestination(destination.destinationId);
  const [showGlobalValidationMessage, setShowGlobalValidationMessage] = useState(false);
  const navigate = useNavigate();

  if (!source || !destination) {
    throw new Error("Source ID and Destination ID are required");
  }

  const methods = useForm<DataActivationConnectionFormValues>({
    defaultValues: streamMappings || { streams: [EMPTY_STREAM] },
    mode: "onChange",
    resolver: zodResolver(DataActivationConnectionFormSchema),
  });

  const { destinationDefinitionMap } = useDestinationDefinitionList();
  const destinationDefinition = destinationDefinitionMap.get(destination.destinationDefinitionId);
  if (!destinationDefinition) {
    throw new Error("Destination definition not found");
  }

  const handleSubmit = (formValues: DataActivationConnectionFormValues) => {
    setStreamMappings(formValues);
    navigate(`${ConnectionRoutePaths.ConfigureContinued}?${searchParams.toString()}`);
  };

  const schemasLoaded = discoveredSource && discoveredDestination;

  return (
    <FormProvider {...methods}>
      <form
        onSubmit={(event) => {
          setShowGlobalValidationMessage(true);
          return methods.handleSubmit(handleSubmit)(event);
        }}
        className={styles.dataActivationMappingPage}
      >
        <CreateConnectionFlowLayout.Main>
          <Box p="xl">
            <FlexContainer direction="column" gap="lg">
              <FlexContainer direction="column" gap="lg">
                <Heading as="h1">
                  <FormattedMessage id="connections.mappings.title" />
                </Heading>
                <FlexContainer justifyContent="space-between">
                  <Text>
                    <FormattedMessage
                      id="connection.dataActivationDescription"
                      values={{
                        destinationName: destinationDefinition.name,
                        sourceName: source.name,
                        bold: (children) => (
                          <Text as="span" bold>
                            {children}
                          </Text>
                        ),
                      }}
                    />
                  </Text>
                  <div style={{ flexShrink: 0 }}>
                    <Text>
                      <ExternalLink href={links.dataActivationDocs}>
                        <FormattedMessage id="connections.mappings.docsLink" /> <Icon type="share" size="xs" />
                      </ExternalLink>
                    </Text>
                  </div>
                </FlexContainer>
              </FlexContainer>
              {!schemasLoaded && <LoadingSchema />}

              {schemasLoaded && discoveredSource?.catalog && (
                <FlexContainer direction="column" gap="lg">
                  <StreamMappings
                    sourceCatalog={discoveredSource.catalog}
                    source={source}
                    destination={destination}
                    destinationCatalog={discoveredDestination.catalog}
                  />
                </FlexContainer>
              )}
            </FlexContainer>
          </Box>
        </CreateConnectionFlowLayout.Main>
        {schemasLoaded && (
          <CreateConnectionFlowLayout.Footer>
            <Link
              to={{
                pathname: createLink(`/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`),
                search: `?${SOURCE_ID_PARAM}=${source.sourceId}`,
              }}
              variant="button"
            >
              <FormattedMessage id="connectionForm.backToDefineDestination" />
            </Link>

            <FlexContainer justifyContent="flex-end" alignItems="center">
              {showGlobalValidationMessage && <GlobalValidationMessage />}
              <Button type="submit">
                <FormattedMessage id="connectionForm.configureConnection" />
              </Button>
            </FlexContainer>
          </CreateConnectionFlowLayout.Footer>
        )}
      </form>
    </FormProvider>
  );
};

const GlobalValidationMessage = () => {
  const { formState } = useFormContext<DataActivationConnectionFormValues>();
  const { isValid } = formState;

  if (isValid) {
    return null;
  }

  return (
    <Text color="red">
      <FormattedMessage id="connectionForm.validation.creationError" />
    </Text>
  );
};
