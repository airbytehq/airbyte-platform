import classNames from "classnames";
import React, { useCallback, useRef, useState } from "react";
import { useInView } from "react-intersection-observer";
import { useResizeDetector } from "react-resize-detector";

import { Tooltip } from "components/ui/Tooltip";

import { Text, TextProps } from "./Text";
import styles from "./TextWithOverflowTooltip.module.scss";

interface TextWithOverflowTooltipProps extends TextProps {
  onlyCheckForSizeIfInView?: boolean;
  ref?: React.MutableRefObject<HTMLElement | null>;
}
export const TextWithOverflowTooltip: React.FC<React.PropsWithChildren<TextWithOverflowTooltipProps>> = ({
  children,
  onlyCheckForSizeIfInView = false,
  className,
  ...props
}) => {
  const [tooltipDisabled, setTooltipDisabled] = useState(true);

  const textRef = useRef<HTMLElement | null>(null);
  const { inView, ref: inViewRef } = useInView({ delay: 1000, skip: !onlyCheckForSizeIfInView });

  const onResize = useCallback(() => {
    if (!onlyCheckForSizeIfInView || inView) {
      setTooltipDisabled((textRef.current?.scrollWidth ?? 0) <= (textRef.current?.clientWidth ?? 0));
    }
  }, [inView, onlyCheckForSizeIfInView]);

  const { ref: resizeRef } = useResizeDetector<HTMLElement>({
    onResize,
    refreshMode: "debounce",
    handleHeight: false,
    handleWidth: !onlyCheckForSizeIfInView || inView,
  });

  return (
    <Tooltip
      control={
        <Text
          ref={(element) => {
            textRef.current = element;
            resizeRef.current = element;
            if (props.ref) {
              props.ref.current = element;
            }
            inViewRef(element);
          }}
          className={classNames(className, styles.overflowText)}
          {...props}
        >
          {children}
        </Text>
      }
      disabled={tooltipDisabled}
      className={styles.tooltip}
      containerClassName={styles.overflowTooltipContainer}
    >
      {textRef.current?.textContent}
    </Tooltip>
  );
};
