import React from "react";

import { LoadingPage } from "components";

import { useResolveNavigate } from "hooks/services/useConnectorAuth";

const CompleteOauthRequest: React.FC = React.memo(() => {
  useResolveNavigate();

  return <LoadingPage />;
});
CompleteOauthRequest.displayName = "CompleteOauthRequest";
export { CompleteOauthRequest };
