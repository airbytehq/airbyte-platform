import type { ConnectorBuilderProjectStreamRead } from "@src/core/api/types/AirbyteClient";

interface GridInput {
  slices: number;
  pagesPerSlice: number;
  recordsPerPage: number;
  /** True when the read stopped at the page, slice or record limit of the test read. */
  limitReached: boolean;
}

/**
 * Results of a Connector Builder test read, for `outcomes.streamRead` in a scenario. The server runs the read with
 * the connector code, so the mock never computes a result from the request. A test declares the result it needs.
 */
export const streamRead = {
  /**
   * A read with the same number of pages in each slice and the same number of records on each page. Record IDs
   * name their slice, page and position, for example "slice-1-page-2-record-1".
   */
  grid: ({ slices, pagesPerSlice, recordsPerPage, limitReached }: GridInput): ConnectorBuilderProjectStreamRead => ({
    logs: [],
    test_read_limit_reached: limitReached,
    slices: Array.from({ length: slices }, (_, slice) => ({
      slice_descriptor: { partition: String(slice + 1) },
      pages: Array.from({ length: pagesPerSlice }, (_, page) => ({
        records: Array.from({ length: recordsPerPage }, (_, record) => ({
          id: `slice-${slice + 1}-page-${page + 1}-record-${record + 1}`,
        })),
        request: { url: "https://api.example.com/items", http_method: "GET" },
        response: { status: 200, body: "{}" },
      })),
    })),
  }),
};
