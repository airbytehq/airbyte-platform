import React, { useEffect, useState } from "react";
import { useLocation } from "react-router-dom";

import { SyncSchemaStream } from "core/domain/catalog";

export const useScrollIntoViewStream = (
  stream: SyncSchemaStream,
  namespaceRef: React.RefObject<HTMLParagraphElement>,
  streamNameRef: React.RefObject<HTMLParagraphElement>
) => {
  const { state } = useLocation();
  const [isVisible, setIsVisible] = useState(false);

  useEffect(() => {
    let timeout: NodeJS.Timeout;

    if (
      typeof state === "object" &&
      state &&
      "streamName" in state &&
      "namespace" in state &&
      state.namespace &&
      state.streamName
    ) {
      const { namespace, streamName } = state;
      if (
        stream.stream?.name === streamName &&
        stream.stream?.namespace === namespace &&
        streamNameRef.current &&
        namespaceRef.current
      ) {
        streamNameRef.current.scrollIntoView({ behavior: "smooth", block: "center" });
        timeout = setTimeout(() => setIsVisible(true), 1500);
      }
    }

    return () => clearTimeout(timeout);
  }, [namespaceRef, state, stream.stream?.name, stream.stream?.namespace, streamNameRef]);

  return { isVisible };
};
