import React from "react";
import { QueryClient, QueryClientProvider } from "react-query";
import { ReactQueryDevtools } from "react-query/devtools";

import { isCloudApp } from "utils/app";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      retry: 0,
      notifyOnChangePropsExclusions: ["isStale"],
    },
  },
});

const StoreProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <QueryClientProvider client={queryClient}>
    <ReactQueryDevtools
      initialIsOpen={false}
      position="bottom-right"
      toggleButtonProps={{
        style: isCloudApp() ? { transform: "translate(-65px, -12px)" } : undefined,
        id: "react-query-devtool-btn",
      }}
    />
    {children}
  </QueryClientProvider>
);

export { StoreProvider };
