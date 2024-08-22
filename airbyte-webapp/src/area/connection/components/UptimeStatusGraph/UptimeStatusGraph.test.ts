import { StreamStatusType } from "components/connection/StreamStatusIndicator";

import { ensureStreams } from "./UptimeStatusGraph";

const bucketWithoutStreams = {
  jobId: 0,
  runtimeMs: 5,
  recordsEmitted: 10,
  recordsCommitted: 9,
} as const;

describe("ensureStreams", () => {
  it("returns the same bucket if 1 real stream is all there is", () => {
    const bucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: "stream1", status: StreamStatusType.Synced }],
    };
    expect(ensureStreams(bucket, 0, { today: bucket }, ["today"])).toBe(bucket);
  });

  it("returns the same bucket if 1 null stream is all there is", () => {
    const bucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: null as unknown as string, status: StreamStatusType.Incomplete }],
    };
    expect(ensureStreams(bucket, 0, { today: bucket }, ["today"])).toBe(bucket);
  });

  it("returns the same bucket if null streams is all there is", () => {
    const bucket1 = {
      ...bucketWithoutStreams,
      streams: [{ streamName: null as unknown as string, status: StreamStatusType.Incomplete }],
    };
    const bucket2 = {
      ...bucketWithoutStreams,
      streams: [{ streamName: null as unknown as string, status: StreamStatusType.Incomplete }],
    };
    expect(ensureStreams(bucket1, 0, { today: bucket1, yesterday: bucket2 }, ["today", "yesterday"])).toBe(bucket1);
    expect(ensureStreams(bucket2, 1, { today: bucket1, yesterday: bucket2 }, ["today", "yesterday"])).toBe(bucket2);
  });

  it("returns the same bucket if the bucket is a real stream", () => {
    const goodBucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: "stream1", status: StreamStatusType.Synced }],
    };
    const badBucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: undefined as unknown as string, status: StreamStatusType.Synced }],
    };
    expect(ensureStreams(goodBucket, 1, { today: goodBucket, yesterday: badBucket }, ["yesterday", "today"])).toBe(
      goodBucket
    );
  });

  it("returns the previous good bucket if the bucket is a null stream", () => {
    const firstBucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: "stream1", status: StreamStatusType.Synced }],
    };
    const badBucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: undefined as unknown as string, status: StreamStatusType.Synced }],
    };
    const lastBucket = {
      ...bucketWithoutStreams,
      streams: [
        { streamName: "stream1", status: StreamStatusType.Synced },
        { streamName: "stream2", status: StreamStatusType.Synced },
      ],
    };

    // first bucket is fine, keep it
    expect(
      ensureStreams(firstBucket, 0, { first: firstBucket, second: badBucket, third: lastBucket }, [
        "first",
        "second",
        "third",
      ])
    ).toBe(firstBucket);

    // middle bucket has a null stream, see it replaced with error'd streams from the first bucket
    expect(
      ensureStreams(badBucket, 1, { first: firstBucket, second: badBucket, third: lastBucket }, [
        "first",
        "second",
        "third",
      ])
    ).toEqual({
      ...badBucket,
      streams: firstBucket.streams.map((stream) => ({ ...stream, status: StreamStatusType.Incomplete })),
    });

    // last bucket is fine, keep it
    expect(
      ensureStreams(lastBucket, 2, { first: firstBucket, second: badBucket, third: lastBucket }, [
        "first",
        "second",
        "third",
      ])
    ).toBe(lastBucket);
  });

  it("returns the next good bucket if the bucket is a null stream and there isn't a previous good one", () => {
    const badBucket = {
      ...bucketWithoutStreams,
      streams: [{ streamName: undefined as unknown as string, status: StreamStatusType.Synced }],
    };
    const lastBucket = {
      ...bucketWithoutStreams,
      streams: [
        { streamName: "stream1", status: StreamStatusType.Synced },
        { streamName: "stream2", status: StreamStatusType.Synced },
      ],
    };

    // first bucket has a null stream, see it replaced with error'd streams from the last bucket
    expect(ensureStreams(badBucket, 0, { first: badBucket, second: lastBucket }, ["first", "second"])).toEqual({
      ...badBucket,
      streams: lastBucket.streams.map((stream) => ({ ...stream, status: StreamStatusType.Incomplete })),
    });

    // last bucket is fine, keep it
    expect(ensureStreams(lastBucket, 1, { first: badBucket, second: lastBucket }, ["first", "second"])).toBe(
      lastBucket
    );
  });
});
