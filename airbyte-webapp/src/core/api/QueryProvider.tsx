import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import React from "react";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      retry: 0,
    },
  },
});

export const QueryProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <QueryClientProvider client={queryClient}>
    <ReactQueryDevtools
      initialIsOpen={false}
      position="bottom-right"
      toggleButtonProps={{
        id: "react-query-devtool-btn",
      }}
    />
    {children}
  </QueryClientProvider>
);
