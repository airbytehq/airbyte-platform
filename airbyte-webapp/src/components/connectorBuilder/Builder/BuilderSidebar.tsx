import classNames from "classnames";
import React, { useCallback, useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { useUpdateEffect } from "react-use";

import Indicator from "components/Indicator";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useCustomComponentsEnabled } from "core/api";
import {
  AsyncRetrieverType,
  CustomRetrieverType,
  DeclarativeStreamType,
  SimpleRetrieverType,
  StateDelegatingStreamType,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import { BuilderView, useConnectorBuilderPermission } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderSidebar.module.scss";
import { Sidebar } from "../Sidebar";
import { DEFAULT_DYNAMIC_STREAM, DEFAULT_SYNC_STREAM, StreamId, getStreamFieldPath } from "../types";
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
  name?: string;
  num: number;
}
const DynamicStreamViewButton: React.FC<DynamicStreamViewButtonProps> = React.memo(({ name, num }) => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");
  const allGeneratedStreams = useBuilderWatch("generatedStreams");
  const generatedStreams = useMemo(() => {
    if (!name) {
      return [];
    }
    return allGeneratedStreams[name];
  }, [allGeneratedStreams, name]);

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
          {generatedStreams && generatedStreams.length > 0 && (
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
      {generatedStreams && generatedStreams.length > 0 && isOpen && (
        <FlexContainer direction="column" gap="none" className={styles.generatedStreamViewContainer}>
          {generatedStreams.map((stream, index) => {
            const streamId: StreamId = { type: "generated_stream", index, dynamicStreamName: name ?? "" };
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
});
DynamicStreamViewButton.displayName = "DynamicStreamViewButton";

interface StreamViewButtonProps {
  num: number;
}
const StreamViewButton: React.FC<StreamViewButtonProps> = React.memo(({ num }) => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const { setValue } = useFormContext();
  const view = useBuilderWatch("view");

  const streamId: StreamId = useMemo(() => ({ type: "stream", index: num }), [num]);
  const streamFieldPath = useCallback((fieldPath?: string) => getStreamFieldPath(streamId, fieldPath), [streamId]);
  const streamName = useBuilderWatch(streamFieldPath("name")) as string | undefined;
  const streamType = useBuilderWatch(streamFieldPath("type")) as
    | DeclarativeStreamType
    | StateDelegatingStreamType
    | undefined;
  const streamRetrieverType = useBuilderWatch(streamFieldPath("retriever.type")) as
    | SimpleRetrieverType
    | AsyncRetrieverType
    | CustomRetrieverType
    | undefined;

  const { getStreamTestWarnings } = useStreamTestMetadata();
  const testWarnings = useMemo(() => getStreamTestWarnings(streamId, true), [getStreamTestWarnings, streamId]);

  const isAsync =
    streamType === DeclarativeStreamType.DeclarativeStream && streamRetrieverType === AsyncRetrieverType.AsyncRetriever;

  return (
    <ViewSelectButton
      data-testid={`navbutton-${String(streamId.index)}`}
      selected={view.type === "stream" && view.index === streamId.index}
      showIndicator={hasErrors([streamId]) ? "error" : testWarnings.length > 0 ? "warning" : undefined}
      onClick={() => {
        setValue("streamTab", "requester");
        setValue("view", streamId);
        analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.STREAM_SELECT, {
          actionDescription: "Stream view selected",
          stream_name: streamName,
        });
      }}
    >
      <FlexContainer className={styles.streamViewButtonContent} alignItems="center">
        {streamName && streamName.trim() ? (
          <Text className={styles.streamViewText}>{streamName}</Text>
        ) : (
          <Text className={styles.emptyStreamViewText}>
            <FormattedMessage id="connectorBuilder.emptyName" />
          </Text>
        )}
        {isAsync && (
          <Text className={styles.asyncBadge}>
            <FormattedMessage id="connectorBuilder.asyncBadge" />
          </Text>
        )}
      </FlexContainer>
    </ViewSelectButton>
  );
});
StreamViewButton.displayName = "StreamViewButton";

interface BuilderSidebarProps {
  className?: string;
}

// Custom hook to extract only the minimal information needed for the sidebar to prevent unnecessary re-renders
const useStreamsSidebarData = () => {
  // Watch only the stream names and structure, not all properties
  const streams = useBuilderWatch("manifest.streams");
  const dynamicStreams = useBuilderWatch("manifest.dynamic_streams");

  // Extract only the data we need for rendering
  const streamData = useMemo(() => {
    return {
      streams:
        streams?.map(({ name }, index) => ({
          name,
          index,
        })) || [],
      dynamicStreams:
        dynamicStreams?.map(({ name }, index) => ({
          name,
          index,
        })) || [],
      streamCount: streams?.length || 0,
      dynamicStreamCount: dynamicStreams?.length || 0,
    };
  }, [streams, dynamicStreams]);

  return streamData;
};

export const BuilderSidebar: React.FC<BuilderSidebarProps> = () => {
  const analyticsService = useAnalyticsService();
  const { hasErrors } = useBuilderErrors();
  const permission = useConnectorBuilderPermission();
  const { setValue, getValues } = useFormContext();
  const view = useBuilderWatch("view");
  const handleViewSelect = (selectedView: BuilderView) => {
    setValue("view", selectedView);
  };

  const specProperties = useBuilderWatch("manifest.spec.connection_specification.properties");

  const { streams, dynamicStreams, streamCount, dynamicStreamCount } = useStreamsSidebarData();

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
                number: Object.keys(specProperties ?? {}).length,
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
              <FormattedMessage id="connectorBuilder.streamsHeading" values={{ number: streamCount }} />
            </Text>
            <InfoTooltip placement="top">
              <FormattedMessage id="connectorBuilder.streamTooltip" />
            </InfoTooltip>
          </FlexContainer>

          <Button
            type="button"
            className={styles.addStreamButton}
            onClick={() => {
              const currentStreams = getValues("manifest.streams") ?? [];
              setValue("manifest.streams", [...currentStreams, DEFAULT_SYNC_STREAM]);
              setValue("view", { type: "stream", index: streamCount });
            }}
            icon="plus"
            disabled={permission === "readOnly"}
          />
        </FlexContainer>

        <FlexContainer direction="column" gap="none" className={styles.streamList}>
          {streams.map(({ name, index }) => (
            <StreamViewButton key={`${name}-${index}`} num={index} />
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
                <FormattedMessage id="connectorBuilder.dynamicStreamsHeading" values={{ number: dynamicStreamCount }} />
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

            <Button
              type="button"
              className={styles.addStreamButton}
              onClick={() => {
                const currentDynamicStreams = getValues("manifest.dynamic_streams") ?? [];
                setValue("manifest.dynamic_streams", [...currentDynamicStreams, DEFAULT_DYNAMIC_STREAM]);
                setValue("view", { type: "dynamic_stream", index: dynamicStreamCount });
              }}
              icon="plus"
              disabled={permission === "readOnly"}
            />
          </FlexContainer>

          <FlexContainer direction="column" gap="none" className={styles.streamList}>
            {dynamicStreams.map(({ name, index }) => (
              <DynamicStreamViewButton key={`${name}-${index}`} name={name} num={index} />
            ))}
          </FlexContainer>
        </FlexContainer>
      )}
    </Sidebar>
  );
};
