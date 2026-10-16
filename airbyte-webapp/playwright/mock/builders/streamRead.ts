import type {
  ConnectorBuilderProjectStreamRead,
  ConnectorBuilderProjectStreamReadInferredSchema,
  ConnectorBuilderProjectStreamReadSlicesItemPagesItemRecordsItem,
} from "@src/core/api/types/AirbyteClient";

type StreamRecord = ConnectorBuilderProjectStreamReadSlicesItemPagesItemRecordsItem;

interface GridInput {
  slices: number;
  pagesPerSlice: number;
  recordsPerPage: number;
  /** True when the read stopped at the page, slice or record limit of the test read. */
  limitReached: boolean;
}

interface PagesInput {
  /** The records of each page, in order. */
  pages: StreamRecord[][];
  /** The schema that the server detected from the records. */
  inferredSchema?: ConnectorBuilderProjectStreamReadInferredSchema;
  /** True when the read stopped at the page, slice or record limit of the test read. Default: false. */
  limitReached?: boolean;
}

interface FailedRequestInput {
  /** The HTTP status that the API of the connector answered. */
  status: number;
  /** The JSON body that the API of the connector answered. */
  body: object;
}

/** A fixed request for every page. The mock does not copy the URL from the read request. */
const pageRequest = { url: "https://api.example.com/items", http_method: "GET" } as const;

/**
 * Results of a Connector Builder test read, for `outcomes.streamRead` in a scenario or for an override of
 * readConnectorBuilderProjectStream. The server runs the read with the connector code, so the mock never computes a
 * result from the request. A test declares the result it needs.
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
        request: pageRequest,
        response: { status: 200, body: "{}" },
      })),
    })),
  }),

  /**
   * A read of a stream without partitions, with the records of each page. The server returns each page of such a
   * stream as a separate slice, so this read has one slice for each page. When the stream has pagination, the webapp
   * groups these slices into one slice with pages.
   */
  pages: ({ pages, inferredSchema, limitReached = false }: PagesInput): ConnectorBuilderProjectStreamRead => ({
    logs: [],
    test_read_limit_reached: limitReached,
    inferred_schema: inferredSchema,
    slices: pages.map((records) => ({
      pages: [{ records, request: pageRequest, response: { status: 200, body: JSON.stringify({ items: records }) } }],
    })),
  }),

  /**
   * A read that stopped at a request that failed. The page of the failed request has no records, and the read logs
   * the error.
   */
  failedRequest: ({ status, body }: FailedRequestInput): ConnectorBuilderProjectStreamRead => ({
    logs: [{ level: "ERROR", message: `Request to ${pageRequest.url} failed with status code ${status}` }],
    test_read_limit_reached: false,
    slices: [{ pages: [{ records: [], request: pageRequest, response: { status, body: JSON.stringify(body) } }] }],
  }),
};
