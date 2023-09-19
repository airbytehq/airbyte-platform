import { pollUntil } from "./pollUntil";

// a toy promise that can be polled for a specific response
const fourZerosAndThenSeven = () => {
  let _callCount = 0;
  return () => Promise.resolve([0, 0, 0, 0, 7][_callCount++]);
};

const truthyResponse = (x: unknown) => !!x;

describe("pollUntil", () => {
  beforeAll(() => {
    jest.useFakeTimers({ doNotFake: ["nextTick"] });
  });

  afterAll(() => {
    jest.useRealTimers();
  });

  describe("when maxTimeoutMs is not provided", () => {
    it("calls the provided apiFn until condition returns true and resolves to its final return value", () => {
      const pollableFn = fourZerosAndThenSeven();
      const result = pollUntil(pollableFn, truthyResponse, { intervalMs: 1 });
      jest.advanceTimersByTime(10);
      return expect(result).resolves.toBe(7);
    });
  });

  describe("when condition returns true before maxTimeoutMs is reached", () => {
    it("calls the provided apiFn until condition returns true and resolves to its final return value", () => {
      const pollableFn = fourZerosAndThenSeven();
      const result = pollUntil(pollableFn, truthyResponse, { intervalMs: 1, maxTimeoutMs: 100 });
      jest.advanceTimersByTime(10);
      return expect(result).resolves.toBe(7);
    });
  });

  describe("when maxTimeoutMs is reached before condition returns true", () => {
    it("resolves to false", () => {
      const pollableFn = fourZerosAndThenSeven();
      const result = pollUntil(pollableFn, truthyResponse, { intervalMs: 100, maxTimeoutMs: 1 });
      jest.advanceTimersByTime(100);
      return expect(result).resolves.toBe(false);
    });

    it("calls its apiFn arg no more than (maxTimeoutMs / intervalMs) times", async () => {
      let _callCount = 0;
      const pollableFn = jest.fn(() => {
        return Promise.resolve([1, 2, 3, 4, 5][_callCount++]);
      });

      const polling = pollUntil(pollableFn, (_) => false, { intervalMs: 20, maxTimeoutMs: 78 });

      // Advance the timer by 20ms each. Make sure to wait one more tick (which isn't using fake timers)
      // so rxjs will actually call pollableFn again (and thus we get the right count on the that function).
      // Without waiting a tick after each advance timer, we'd effectively just advance by 80ms and
      // not call the pollableFn multiple times, because the maxTimeout logic would be triggered which
      // would cause the subsequent pollableFn calls to not be properly processed.
      jest.advanceTimersByTime(20);
      await new Promise(process.nextTick);
      jest.advanceTimersByTime(20);
      await new Promise(process.nextTick);
      jest.advanceTimersByTime(20);
      await new Promise(process.nextTick);
      jest.advanceTimersByTime(20);
      await new Promise(process.nextTick);

      const result = await polling;

      expect(result).toBe(false);
      expect(pollableFn).toHaveBeenCalledTimes(4);
    });
  });
});
