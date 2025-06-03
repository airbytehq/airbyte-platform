import classNames from "classnames";
import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useUpdateEffect } from "react-use";

import Indicator from "components/Indicator";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCustomComponentsEnabled } from "core/api";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import {
  BuilderView,
  useConnectorBuilderFormState,
} from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import { AddStreamButton } from "./AddStreamButton";
import styles from "./BuilderSidebar.module.scss";
import { Sidebar } from "../Sidebar";
import { GeneratedBuilderStream, StreamId } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderWatch } from "../useBuilderWatch";
import { useStreamTestMetadata } from "../useStreamTestMetadata";

interface ViewSelectButtonProps {
  className?: string;
  selected: boolean;
  showIndicator?: "warning" | "error";
  onClick: () => void;
  "data-testid": string;
}

export const ViewSelectButton: React.FC<React.PropsWithChildren<ViewSelectButtonProps>> = ({
  children,
  className,
  selected,
  showIndicator,
  onClick,
  "data-testid": testId,
}) => {
  return (
    <button
      type="button"
      data-testid={testId}
      className={classNames(className, styles.viewButton, {
        [styles.selectedViewButton]: selected,
        [styles.unselectedViewButton]: !selected,
      })}
      onClick={onClick}
    >
      <div className={styles.viewLabel}>{children}</div>
      {showIndicator && (
        <Indicator
          className={classNames(styles.indicator, { [styles.warningIndicator]: showIndicator === "warning" })}
        />
      )}
    </button>
  );
};

interface DynamicStreamViewButtonProps {
  name: string;
  num: number;
}
const DynamicStreamViewButton: React.FC<DynamicStreamViewButtonProps> = ({ name, num }) => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");
  const generatedStreams: GeneratedBuilderStream[] | undefined = useBuilderWatch("formValues.generatedStreams")[name];

  const { getStreamTestWarnings } = useStreamTestMetadata();
  const testWarnings = useMemo(
    () => getStreamTestWarnings({ type: "dynamic_stream", index: num }, true),
    [getStreamTestWarnings, num]
  );

  const [isOpen, setIsOpen] = useState(false);

  const testStreamId = useBuilderWatch("testStreamId");
  // If the number of generated streams for the current dynamic stream changes, expand its generated streams list
  useUpdateEffect(() => {
    if (testStreamId.type === "dynamic_stream" && testStreamId.index === num && generatedStreams?.length) {
      setIsOpen(true);
    }
  }, [generatedStreams?.length]);

  const viewId: StreamId = { type: "dynamic_stream", index: num };

  return (
    <FlexContainer direction="column" gap="none">
      <ViewSelectButton
        data-testid={`navbutton-${String(num)}`}
        selected={view.type === "dynamic_stream" && view.index === num}
        showIndicator={hasErrors([viewId]) ? "error" : testWarnings.length > 0 ? "warning" : undefined}
        onClick={() => {
          setValue("view", viewId);
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_SELECT, {
            actionDescription: "Dynamic stream view selected",
            dynamicStreamName: name,
          });
        }}
      >
        <FlexContainer className={styles.streamViewButtonContent} alignItems="center">
          {generatedStreams && (
            <Icon type={isOpen ? "chevronDown" : "chevronRight"} onClick={() => setIsOpen((isOpen) => !isOpen)} />
          )}
          {name && name.trim() ? (
            <Text className={styles.streamViewText}>{name}</Text>
          ) : (
            <Text className={styles.emptyStreamViewText}>
              <FormattedMessage id="connectorBuilder.emptyName" />
            </Text>
          )}
        </FlexContainer>
      </ViewSelectButton>
      {generatedStreams && isOpen && (
        <FlexContainer direction="column" gap="none" className={styles.generatedStreamViewContainer}>
          {generatedStreams.map((stream, index) => {
            const streamId: StreamId = { type: "generated_stream", index, dynamicStreamName: name };
            return (
              <ViewSelectButton
                key={stream.name}
                data-testid={`navbutton-${String(num)}`}
                selected={view.type === "generated_stream" && view.index === index}
                showIndicator={getStreamTestWarnings(streamId, true).length > 0 ? "warning" : undefined}
                onClick={() => {
                  setValue("view", streamId);
                  analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DYNAMIC_STREAM_SELECT, {
                    actionDescription: "Dynamic stream view selected",
                    dynamicStreamName: name,
                  });
                }}
              >
                <Text className={styles.streamViewText}>{stream.name}</Text>
              </ViewSelectButton>
            );
          })}
        </FlexContainer>
      )}
    </FlexContainer>
  );
};

interface StreamViewButtonProps {
  id: string;
  name: string;
  num: number;
  async: boolean;
}
const StreamViewButton: React.FC<StreamViewButtonProps> = ({ id, name, num, async }) => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");

  const { getStreamTestWarnings } = useStreamTestMetadata();
  const testWarnings = useMemo(
    () => getStreamTestWarnings({ type: "stream", index: num }, true),
    [getStreamTestWarnings, num]
  );

  const viewId: StreamId = { type: "stream", index: num };

  return (
    <ViewSelectButton
      data-testid={`navbutton-${String(num)}`}
      selected={view.type === "stream" && view.index === num}
      showIndicator={hasErrors([viewId]) ? "error" : testWarnings.length > 0 ? "warning" : undefined}
      onClick={() => {
        setValue("streamTab", "requester");
        setValue("view", viewId);
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_SELECT, {
          actionDescription: "Stream view selected",
          stream_id: id,
          stream_name: name,
        });
      }}
    >
      <FlexContainer className={styles.streamViewButtonContent} alignItems="center">
        {name && name.trim() ? (
          <Text className={styles.streamViewText}>{name}</Text>
        ) : (
          <Text className={styles.emptyStreamViewText}>
            <FormattedMessage id="connectorBuilder.emptyName" />
          </Text>
        )}
        {async && <Text className={styles.asyncBadge}>async</Text>}
      </FlexContainer>
    </ViewSelectButton>
  );
};

interface BuilderSidebarProps {
  className?: string;
}

export const BuilderSidebar: React.FC<BuilderSidebarProps> = () => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { permission } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const formValues = useBuilderWatch("formValues");
  const view = useBuilderWatch("view");
  const handleViewSelect = (selectedView: BuilderView) => {
    setValue("view", selectedView);
  };

  const areDynamicStreamsEnabled = useExperiment("connectorBuilder.dynamicStreams");
  const areCustomComponentsEnabled = useCustomComponentsEnabled();
  const customComponentsCodeValue = useBuilderWatch("customComponentsCode");

  // We want to show the custom components tab any time the custom components code is set.
  // This is to ensure a user can still remove the custom components code if they want to (in the event of a fork).
  const showCustomComponentsTab = areCustomComponentsEnabled || customComponentsCodeValue;

  return (
    <Sidebar yamlSelected={false}>
      <FlexContainer direction="column" alignItems="stretch" gap="none">
        <ViewSelectButton
          data-testid="navbutton-global"
          selected={view.type === "global"}
          showIndicator={hasErrors([{ type: "global" }]) ? "error" : undefined}
          onClick={() => {
            handleViewSelect({ type: "global" });
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.GLOBAL_CONFIGURATION_SELECT, {
              actionDescription: "Global Configuration view selected",
            });
          }}
        >
          <Icon type="parameters" />
          <Text className={styles.streamViewText}>
            <FormattedMessage id="connectorBuilder.globalConfiguration" />
          </Text>
        </ViewSelectButton>

        <ViewSelectButton
          data-testid="navbutton-inputs"
          selected={view.type === "inputs"}
          showIndicator={hasErrors([{ type: "inputs" }]) ? "error" : undefined}
          onClick={() => {
            handleViewSelect({ type: "inputs" });
            analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.USER_INPUTS_SELECT, {
              actionDescription: "User Inputs view selected",
            });
          }}
        >
          <Icon type="user" />
          <Text className={styles.streamViewText}>
            <FormattedMessage
              id="connectorBuilder.userInputs"
              values={{
                number: formValues.inputs.filter(({ definition }) => !definition.airbyte_hidden).length,
              }}
            />
          </Text>
        </ViewSelectButton>

        {showCustomComponentsTab && (
          <ViewSelectButton
            data-testid="navbutton-components"
            selected={view.type === "components"}
            onClick={() => {
              handleViewSelect({ type: "components" });
              analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.COMPONENTS_SELECT, {
                actionDescription: "Components view selected",
              });
            }}
          >
            <Icon type="wrench" />
            <Text className={styles.streamViewText}>
              <FormattedMessage id="connectorBuilder.customComponents" />
            </Text>
          </ViewSelectButton>
        )}
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="sm" className={styles.streamListContainer}>
        <FlexContainer className={styles.streamsHeader} alignItems="center" justifyContent="space-between">
          <FlexContainer alignItems="center" gap="none">
            <Text className={styles.streamsHeading} size="xs" bold>
              <FormattedMessage id="connectorBuilder.streamsHeading" values={{ number: formValues.streams.length }} />
            </Text>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.streamTooltip" />
            </InfoTooltip>
          </FlexContainer>

          <AddStreamButton
            streamType="stream"
            onAddStream={(addedStreamNum) => handleViewSelect({ type: "stream", index: addedStreamNum })}
            disabled={permission === "readOnly"}
            data-testid="add-stream"
          />
        </FlexContainer>

        <FlexContainer direction="column" gap="none" className={styles.streamList}>
          {formValues.streams.map(({ name, id, requestType }, num) => (
            <StreamViewButton key={id} id={id} name={name} num={num} async={requestType === "async"} />
          ))}
        </FlexContainer>
      </FlexContainer>

      {areDynamicStreamsEnabled && (
        <FlexContainer
          direction="column"
          alignItems="stretch"
          gap="sm"
          className={classNames(styles.streamListContainer, styles.streamTemplateList)}
        >
          <FlexContainer className={styles.streamsHeader} alignItems="center" justifyContent="space-between">
            <FlexContainer alignItems="center" gap="none">
              <Text className={styles.streamsHeading} size="xs" bold>
                <FormattedMessage
                  id="connectorBuilder.dynamicStreamsHeading"
                  values={{ number: formValues.dynamicStreams.length }}
                />
              </Text>
              <InfoTooltip placement="top">
                <FormattedMessage
                  id="connectorBuilder.dynamicStreamTooltip"
                  values={{
                    a: (node: React.ReactNode) => (
                      <a href={links.connectorBuilderStreamTemplates} target="_blank" rel="noreferrer">
                        {node}
                      </a>
                    ),
                  }}
                />
              </InfoTooltip>
            </FlexContainer>

            <AddStreamButton
              streamType="dynamicStream"
              onAddStream={(addedStreamNum) => handleViewSelect({ type: "dynamic_stream", index: addedStreamNum })}
              disabled={permission === "readOnly"}
              data-testid="add-dynamic-stream"
            />
          </FlexContainer>

          <FlexContainer direction="column" gap="none" className={styles.streamList}>
            {formValues.dynamicStreams.map(({ dynamicStreamName }, num) => (
              <DynamicStreamViewButton key={dynamicStreamName} name={dynamicStreamName} num={num} />
            ))}
          </FlexContainer>
        </FlexContainer>
      )}
    </Sidebar>
  );
};
