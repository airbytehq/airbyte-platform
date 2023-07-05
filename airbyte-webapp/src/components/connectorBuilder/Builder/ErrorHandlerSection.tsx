import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { DefaultErrorHandlerBackoffStrategiesItem, HttpResponseFilter } from "core/request/ConnectorManifest";
import { links } from "utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getOptionsByManifest } from "./manifestHelpers";
import { ToggleGroupField } from "./ToggleGroupField";
import { StreamPathFn } from "../types";

interface PartitionSectionProps {
  streamFieldPath: StreamPathFn;
  currentStreamIndex: number;
}

export const ErrorHandlerSection: React.FC<PartitionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();

  const getBackoffOptions = (buildPath: (path: string) => string): OneOfOption[] => [
    {
      label: "Constant",
      typeValue: "ConstantBackoffStrategy",
      default: {
        backoff_time_in_seconds: 5,
      },
      children: (
        <BuilderField
          type="number"
          path={buildPath("backoff_strategy.backoff_time_in_seconds")}
          manifestPath="ConstantBackoffStrategy.properties.backoff_time_in_seconds"
        />
      ),
    },
    {
      label: "Exponential",
      typeValue: "ExponentialBackoffStrategy",
      default: {
        factor: "",
      },
      children: (
        <BuilderField
          type="number"
          path={buildPath("backoff_strategy.factor")}
          manifestPath="ExponentialBackoffStrategy.properties.factor"
          optional
        />
      ),
    },
    {
      label: "Wait time from header",
      typeValue: "WaitTimeFromHeader",
      default: {
        header: "",
        regex: "",
      },
      children: (
        <>
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.header")}
            manifestPath="WaitTimeFromHeader.properties.header"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.regex")}
            optional
            manifestPath="WaitTimeFromHeader.properties.regex"
          />
        </>
      ),
    },
    {
      label: "Wait until time from header",
      typeValue: "WaitUntilTimeFromHeader",
      default: {
        header: "",
        regex: "",
        min_wait: "",
      },
      children: (
        <>
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.header")}
            manifestPath="WaitUntilTimeFromHeader.properties.header"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.regex")}
            optional
            manifestPath="WaitUntilTimeFromHeader.properties.regex"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.min_wait")}
            optional
            manifestPath="WaitUntilTimeFromHeader.properties.min_wait"
          />
        </>
      ),
    },
  ];

  return (
    <BuilderCard
      docLink={links.connectorBuilderErrorHandler}
      label="Error Handler"
      tooltip={getDescriptionByManifest("DefaultErrorHandler")}
      toggleConfig={{
        path: streamFieldPath("errorHandler"),
        defaultValue: [
          {
            type: "DefaultErrorHandler",
          },
        ],
      }}
      copyConfig={{
        path: "errorHandler",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromErrorHandlerTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToErrorHandlerTitle" }),
      }}
    >
      <Message type="warning" text={<FormattedMessage id="connectorBuilder.errorHandlerWarning" />} />
      <BuilderList
        basePath={streamFieldPath("errorHandler")}
        emptyItem={{
          type: "DefaultErrorHandler",
          response_filters: [],
        }}
        addButtonLabel="Add error handler"
      >
        {({ buildPath }) => (
          <FlexContainer direction="column">
            <ToggleGroupField<DefaultErrorHandlerBackoffStrategiesItem>
              label="Backoff Strategy"
              tooltip="Optionally configures how to retry a request multiple times"
              fieldPath={buildPath("backoff_strategy")}
              initialValues={{
                type: "ConstantBackoffStrategy",
                backoff_time_in_seconds: 5,
              }}
            >
              <BuilderOneOf
                path={buildPath("backoff_strategy")}
                label="Strategy"
                tooltip="The strategy to use to decide when to retry a request"
                manifestOptionPaths={[
                  "ConstantBackoffStrategy",
                  "ExponentialBackoffStrategy",
                  "WaitTimeFromHeader",
                  "WaitUntilTimeFromHeader",
                ]}
                options={getBackoffOptions(buildPath)}
              />
            </ToggleGroupField>
            <ToggleGroupField<HttpResponseFilter>
              label="Response Filter"
              tooltip="Specify a filter to specify how to handle certain requests"
              fieldPath={buildPath("response_filter")}
              initialValues={{
                type: "HttpResponseFilter",
                action: "IGNORE",
              }}
            >
              <>
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.error_message_contains")}
                  optional
                  manifestPath="HttpResponseFilter.properties.error_message_contains"
                />
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.predicate")}
                  optional
                  pattern="{{ predicate logic }}"
                  manifestPath="HttpResponseFilter.properties.predicate"
                />
                <BuilderField
                  type="array"
                  path={buildPath("response_filter.http_codes")}
                  itemType="integer"
                  optional
                  manifestPath="HttpResponseFilter.properties.http_codes"
                />
                <BuilderField
                  type="enum"
                  path={buildPath("response_filter.action")}
                  options={getOptionsByManifest("HttpResponseFilter.properties.action")}
                  manifestPath="HttpResponseFilter.properties.action"
                />
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.error_message")}
                  optional
                  manifestPath="HttpResponseFilter.properties.error_message"
                />
              </>
            </ToggleGroupField>
          </FlexContainer>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
