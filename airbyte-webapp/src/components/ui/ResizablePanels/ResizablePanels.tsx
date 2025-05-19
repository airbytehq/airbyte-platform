import classNames from "classnames";
import React from "react";
import { ReflexContainer, ReflexElement, ReflexSplitter } from "react-reflex";

import { Heading } from "components/ui/Heading";

import styles from "./ResizablePanels.module.scss";

interface ResizablePanelsProps {
  className?: string;
  orientation?: "vertical" | "horizontal";
  panels: PanelProps[];
  onlyShowFirstPanel?: boolean;
}

interface PanelProps {
  children: React.ReactNode;
  minWidth: number;
  className?: string;
  flex?: number;
  overlay?: Overlay;
  onStopResize?: (newFlex: number | undefined) => void;
  splitter?: React.ReactNode;
}

interface Overlay {
  displayThreshold: number;
  header: string;
  rotation?: "clockwise" | "counter-clockwise";
}

interface PanelContainerProps {
  className?: string;
  dimensions?: {
    width: number;
    height: number;
  };
  overlay?: Overlay;
}

const PanelContainer: React.FC<React.PropsWithChildren<PanelContainerProps>> = ({
  children,
  className,
  dimensions,
  overlay,
}) => {
  const width = dimensions?.width ?? 0;
  const showOverlay = overlay && Boolean(width) && width <= overlay.displayThreshold;

  return (
    <div className={classNames(className, styles.panelContainer)}>
      {showOverlay ? (
        <div className={styles.lightOverlay}>
          <Heading
            as="h2"
            className={classNames(styles.rotatedHeader, {
              [styles.counterClockwise]: overlay?.rotation === "counter-clockwise",
            })}
          >
            {overlay.header}
          </Heading>
        </div>
      ) : (
        children
      )}
    </div>
  );
};

export const ResizablePanels: React.FC<ResizablePanelsProps> = ({
  className,
  orientation = "vertical",
  panels,
  onlyShowFirstPanel = false,
}) => {
  if (panels.length === 0) {
    return null;
  }

  const panel = (panelProps: PanelProps, key: string | number, hidden = false, fullWidth = false) => (
    <ReflexElement
      key={key}
      className={classNames(styles.panelStyle, panelProps.className, {
        [styles.fullWidth]: fullWidth,
        [styles.hidden]: hidden,
      })}
      propagateDimensions
      minSize={panelProps.minWidth}
      flex={panelProps.flex}
      onStopResize={(args) => {
        panelProps.onStopResize?.(args.component.props.flex);
      }}
    >
      {!hidden && <PanelContainer overlay={panelProps.overlay}>{panelProps.children}</PanelContainer>}
    </ReflexElement>
  );

  const splitter = (key: string | number, splitter: React.ReactNode, hidden = false) => (
    <ReflexSplitter key={key} className={classNames(styles.splitter, { [styles.hidden]: hidden })} propagate>
      {splitter ? (
        splitter
      ) : (
        <div
          className={classNames({
            [styles.panelGrabberVertical]: orientation === "vertical",
            [styles.panelGrabberHorizontal]: orientation === "horizontal",
          })}
        >
          <div
            className={classNames(styles.handleIcon, {
              [styles.handleIconVertical]: orientation === "vertical",
              [styles.handleIconHorizontal]: orientation === "horizontal",
            })}
          />
        </div>
      )}
    </ReflexSplitter>
  );

  const children = panels.slice(1).reduce(
    (acc, panelProps, index) => {
      return [
        ...acc,
        splitter(`splitter-${index}`, panelProps.splitter, onlyShowFirstPanel),
        panel(panelProps, `panel-${index}`, onlyShowFirstPanel),
      ];
    },
    [panel(panels[0], "panel-first", false, onlyShowFirstPanel)]
  );

  return (
    <ReflexContainer className={className} orientation={orientation}>
      {children}
    </ReflexContainer>
  );
};
