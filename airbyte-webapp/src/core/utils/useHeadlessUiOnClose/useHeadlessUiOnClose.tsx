import { useEffect, useRef } from "react";

export const DATA_HEADLESS_UI_STATE_ATTRIBUTE = "data-headlessui-state";

/**
 * Allows you to add an onClose handler to a Headless UI component. This is not supported by headlessui out of the box.
 *
 * @param onCloseCallback - Callback to be called when the Headless UI component is closed
 * @returns targetRef - Ref to add to the parent element of the Headless UI component
 * @example
 * ```tsx
 * const { targetRef } = useHeadlessUiOnClose(() => alert("Closed!"));
 *
 * return (
 *  <ListBox ref={targetRef}>
 *     ...
 *  </ListBox>
 * )
 * ```
 */
export const useHeadlessUiOnClose = (onCloseCallback?: () => void) => {
  const targetRef = useRef<HTMLElement>(null);

  useEffect(() => {
    if (!targetRef.current) {
      return;
    }

    const observer = new MutationObserver((mutationsList) => {
      for (const mutation of mutationsList) {
        if (mutation.type === "attributes" && mutation.attributeName === DATA_HEADLESS_UI_STATE_ATTRIBUTE) {
          const currentState = targetRef.current?.getAttribute(DATA_HEADLESS_UI_STATE_ATTRIBUTE);
          const previousState = mutation.oldValue;

          if (previousState === "open" && currentState === "") {
            onCloseCallback?.();
          }
        }
      }
    });

    observer.observe(targetRef.current, {
      attributes: true,
      attributeOldValue: true,
      attributeFilter: [DATA_HEADLESS_UI_STATE_ATTRIBUTE],
    });

    return () => {
      observer.disconnect();
    };
  }, [onCloseCallback]);

  return { targetRef };
};
