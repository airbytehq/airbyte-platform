import { useEffect, useState } from "react";
import { useLocation, useNavigate, Location } from "react-router-dom";

import { SyncSchemaStream } from "core/domain/catalog";

interface RedirectionLocationState {
  namespace?: string;
  streamName?: string;
  action?: "showInReplicationTable" | "openDetails";
}

export interface LocationWithState extends Location {
  state: RedirectionLocationState;
}

export const useRedirectedReplicationStream = (stream: SyncSchemaStream) => {
  const { state, pathname } = useLocation() as LocationWithState;
  const navigate = useNavigate();

  const [doesStreamExist, setDoesStreamExist] = useState(false);
  const [redirectionAction, setRedirectionAction] = useState<"showInReplicationTable" | "openDetails" | undefined>();

  const isLocationStateValid =
    state &&
    typeof state === "object" &&
    "streamName" in state &&
    "action" in state &&
    state.streamName &&
    state.action;

  const { name, namespace } = stream.stream ?? {};

  const isDesiredStream = isLocationStateValid && state?.namespace === namespace && state?.streamName === name;

  useEffect(() => {
    if (isLocationStateValid && isDesiredStream) {
      setRedirectionAction(state?.action);
      setDoesStreamExist(true);
      navigate(pathname, { replace: true });
    }
  }, [isLocationStateValid, isDesiredStream, state?.action, navigate, pathname]);

  return { doesStreamExist, redirectionAction };
};
