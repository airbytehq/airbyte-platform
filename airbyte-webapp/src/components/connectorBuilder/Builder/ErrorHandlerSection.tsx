import { useField } from "formik";
import { useIntl } from "react-intl";

import { ControlLabels } from "components/LabeledControl";
import { FlexContainer } from "components/ui/Flex";

import { DefaultErrorHandlerBackoffStrategiesItem, HttpResponseFilter } from "core/request/ConnectorManifest";

import { BuilderCard } from "./BuilderCard";
import { BuilderField } from "./BuilderField";
import { BuilderList } from "./BuilderList";
import { BuilderOneOf, OneOfOption } from "./BuilderOneOf";
import { ToggleGroupField } from "./ToggleGroupField";
import { BuilderStream } from "../types";

interface PartitionSectionProps {
  streamFieldPath: (fieldPath: string) => string;
  currentStreamIndex: number;
}

export const ErrorHandlerSection: React.FC<PartitionSectionProps> = ({ streamFieldPath, currentStreamIndex }) => {
  const { formatMessage } = useIntl();
  const [field, , helpers] = useField<BuilderStream["errorHandler"]>(streamFieldPath("errorHandler"));

  const handleToggle = (newToggleValue: boolean) => {
    if (newToggleValue) {
      helpers.setValue([
        {
          type: "DefaultErrorHandler",
        },
      ]);
    } else {
      helpers.setValue(undefined);
    }
  };
  const toggledOn = field.value !== undefined;

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
          label="Backoff time in seconds"
          tooltip="Retry again after a fixed number of seconds"
        />
      ),
    },
    {
      label: "Exponential",
      typeValue: "ExponentialBackoffStrategy",
      default: {},
      children: (
        <BuilderField
          type="number"
          path={buildPath("backoff_strategy.factor")}
          label="Multiplier"
          optional
          tooltip="A factor to control how quickly the wait time increases - used in the formula 'multiplier times 2 to the power of (# retry attempt)' seconds"
        />
      ),
    },
    {
      label: "Wait time from header",
      typeValue: "WaitTimeFromHeader",
      default: {},
      children: (
        <>
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.header")}
            label="Header"
            tooltip="The header field in the HTTP response to check for a number of seconds to wait until the next request"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.regex")}
            optional
            label="Regex"
            tooltip="A regex to extract the number of seconds out of the header"
          />
        </>
      ),
    },
    {
      label: "Wait until time from header",
      typeValue: "WaitUntilTimeFromHeader",
      default: {},
      children: (
        <>
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.header")}
            label="Header"
            tooltip="Extract time at which we can retry the request from response header and wait for the difference between now and that time"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.regex")}
            optional
            label="Regex"
            tooltip="A regex to extract the timestamp when to retry out of the hedaer"
          />
          <BuilderField
            type="string"
            path={buildPath("backoff_strategy.min_wait")}
            optional
            label="Minimum wait time"
            tooltip="The minimum amount of time to wait before the next request"
          />
        </>
      ),
    },
  ];

  return (
    <BuilderCard
      toggleConfig={{
        label: (
          <ControlLabels
            label="Error handler"
            infoTooltipContent="Configure how to handle various errors in order to increase the chance for a successful sync"
          />
        ),
        toggledOn,
        onToggle: handleToggle,
      }}
      copyConfig={{
        path: "errorHandler",
        currentStreamIndex,
        copyFromLabel: formatMessage({ id: "connectorBuilder.copyFromErrorHandlerTitle" }),
        copyToLabel: formatMessage({ id: "connectorBuilder.copyToErrorHandlerTitle" }),
      }}
    >
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
              label="Backoff strategy"
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
                options={getBackoffOptions(buildPath)}
              />
            </ToggleGroupField>
            <ToggleGroupField<HttpResponseFilter>
              label="Response filter"
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
                  label="If error message matches"
                  optional
                  tooltip="If set, this string is searched for in the response - if found the filter matches"
                />
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.predicate")}
                  label="and predicate is fulfilled"
                  optional
                  tooltip="If set, logic in double curly braces is interpreted - if a non-empty result is returned, the filter matches"
                  pattern="{{ predicate logic }}"
                />
                <BuilderField
                  type="array"
                  path={buildPath("response_filter.http_codes")}
                  label="and HTTP codes match"
                  itemType="integer"
                  optional
                  tooltip="If set and the response status code matches one of the specified status codes, the filter matches"
                />
                <BuilderField
                  type="enum"
                  path={buildPath("response_filter.action")}
                  label="Then execute action"
                  tooltip="The action to take if the specified filter matches"
                  options={["IGNORE", "SUCCESS", "FAIL", "RETRY"]}
                />
                <BuilderField
                  type="string"
                  path={buildPath("response_filter.error_message")}
                  label="Error message"
                  optional
                  tooltip="The error message shown in the logs of the connector for troubleshooting"
                />
              </>
            </ToggleGroupField>
          </FlexContainer>
        )}
      </BuilderList>
    </BuilderCard>
  );
};
