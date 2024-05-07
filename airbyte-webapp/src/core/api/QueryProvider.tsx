import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import React from "react";

import { HttpError } from "./errors";
import styles from "./QueryProvider.module.scss";

const RETRY_COUNT = 3;

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      retry: (failureCount, error) => {
        if (failureCount < RETRY_COUNT && error instanceof HttpError) {
          if (error.status === 502 || error.status === 503) {
            console.log(
              `🔁 Retrying request to ${error.request.url} due to temporarily unavailable server (HTTP ${
                error.status
              }). Retry ${failureCount + 1}/${RETRY_COUNT}`
            );
            return true;
          }
          if (error.status === 401) {
            console.log(
              `🔁 Retrying request to ${error.request.url} due to unauthorized (HTTP 401). Retry ${
                failureCount + 1
              }/${RETRY_COUNT}`
            );
            return true;
          }
        }
        return false;
      },
    },
  },
});

export const QueryProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => (
  <QueryClientProvider client={queryClient}>
    <div className={styles.devToolsWrapper}>
      <ReactQueryDevtools
        initialIsOpen={false}
        position="bottom-right"
        toggleButtonProps={{
          id: "react-query-devtool-btn",
        }}
      />
    </div>
    {children}
  </QueryClientProvider>
);
