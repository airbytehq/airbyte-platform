import { Row } from "@tanstack/react-table";
import { useEffect, useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { FlatIndexLocationWithAlign } from "react-virtuoso";

import { LocationWithState } from "../../SyncCatalogCard";
import { isSameSyncStream } from "../../utils";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

/**
 * Hook to get the row index(in case if it exists in locationState) to scroll to the row in the table
 * @param rows
 */
export const useInitialRowIndex = (rows: Array<Row<SyncCatalogUIModel>>): FlatIndexLocationWithAlign | undefined => {
  const { state: locationState, pathname } = useLocation() as LocationWithState;
  const navigate = useNavigate();

  useEffect(() => {
    if (!locationState || locationState?.action !== "editStream") {
      return;
    }
    const locationStateResetTimeout = window.setTimeout(
      () => {
        // remove the redirection info from the location state
        navigate(pathname, { replace: true });
      },
      2000 // highlight animation duration
    );

    return () => {
      window.clearTimeout(locationStateResetTimeout);
    };
  }, [locationState, navigate, pathname]);

  return useMemo(() => {
    if (!locationState || locationState?.action !== "editStream") {
      return;
    }

    return {
      index: rows.findIndex((row) => {
        // namespace rows don't have streamNode
        if (!row.original?.streamNode) {
          return false;
        }
        return isSameSyncStream(row.original.streamNode, locationState?.streamName, locationState?.namespace);
      }),
      align: "center",
    };
  }, [locationState, rows]);
};
