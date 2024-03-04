import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { DefaultErrorHandlerBackoffStrategiesItem, HttpResponseFilter } from "core/api/types/ConnectorManifest";
import { links } from "core/utils/links";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { getDescriptionByManifest, getOptionsByManifest } from "./manifestHelpers";
import { ToggleGroupField } from "./ToggleGroupField";

type ErrorHandlerSectionProps =
  | {
      inline: false;
      basePath: `formValues.streams.${number}.errorHandler`;
      currentStreamIndex: number;
    }
  | {
      inline: true;
      basePath: "formValues.global.authenticator.login_requester.errorHandler";
    };

export const ErrorHandlerSection: React.FC<ErrorHandlerSectionProps> = (props) => {
  const { formatMessage } = useIntl();

  const getBackoffOptions = (
    buildPath: (path: string) => string
  ): Array<OneOfOption<DefaultErrorHandlerBackoffStrategiesItem>> => [
    {
      label: formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.strategy.constant" }),
      default: {
        type: "ConstantBackoffStrategy",
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
      label: formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.strategy.exponential" }),
      default: {
        type: "ExponentialBackoffStrategy",
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
      label: formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.strategy.waitTimeFromHeader" }),
      default: {
        type: "WaitTimeFromHeader",
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
      label: formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.strategy.waitUntilTimeFromHeader" }),
      default: {
        type: "WaitUntilTimeFromHeader",
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

  const content = (
    <>
      <Message type="warning" text={<FormattedMessage id="connectorBuilder.errorHandlerWarning" />} />
      <BuilderList
        // cast to strings necessary to avoid a "type union that is too complex to represent"
        basePath={props.basePath}
        emptyItem={{
          type: "DefaultErrorHandler",
          response_filters: [],
        }}
        addButtonLabel={formatMessage({ id: "connectorBuilder.errorHandler.addButton" })}
      >
        {({ buildPath }) => (
          <FlexContainer direction="column">
            <ToggleGroupField<DefaultErrorHandlerBackoffStrategiesItem>
              label={formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.tooltip" })}
              fieldPath={buildPath("backoff_strategy")}
              initialValues={{
                type: "ConstantBackoffStrategy",
                backoff_time_in_seconds: 5,
              }}
            >
              <BuilderOneOf<DefaultErrorHandlerBackoffStrategiesItem>
                path={buildPath("backoff_strategy")}
                label={formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.strategy.label" })}
                tooltip={formatMessage({ id: "connectorBuilder.errorHandler.backoffStrategy.strategy.tooltip" })}
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
              label={formatMessage({ id: "connectorBuilder.errorHandler.responseFilter.label" })}
              tooltip={formatMessage({ id: "connectorBuilder.errorHandler.responseFilter.tooltip" })}
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
                  pattern={formatMessage({ id: "connectorBuilder.errorHandler.responseFilter.predicate.pattern" })}
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
    </>
  );

  return props.inline ? (
    <FlexContainer direction="column" gap="xl">
      {content}
    </FlexContainer>
  ) : (
    <BuilderCard
      docLink={links.connectorBuilderErrorHandler}
      label={formatMessage({ id: "connectorBuilder.errorHandler.label" })}
      tooltip={getDescriptionByManifest("DefaultErrorHandler")}
      toggleConfig={{
        path: props.basePath,
        defaultValue: [
          {
            type: "DefaultErrorHandler",
          },
        ],
      }}
      copyConfig={{
        path: "errorHandler",
        currentStreamIndex: props.currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromErrorHandlerTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToErrorHandlerTitle" }),
      }}
    >
      {content}
    </BuilderCard>
  );
};
