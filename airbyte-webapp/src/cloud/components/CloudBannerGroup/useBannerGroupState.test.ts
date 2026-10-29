import { act, renderHook } from "@testing-library/react";

import { useBannerGroupState } from "./useBannerGroupState";

const STORAGE_KEY = "airbyte_banner-group-state";

let mockUserId = "user-1";

jest.mock("core/services/auth", () => ({
  useCurrentUser: () => ({ userId: mockUserId }),
}));

const persist = (state: Record<string, { isCollapsed: boolean; seenIds: string[] }>) =>
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
const persistCollapsed = (...seenIds: string[]) => persist({ "user-1": { isCollapsed: true, seenIds } });
const readPersisted = () => JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "{}");

const renderState = (registeredIds: string[] = []) => {
  const hook = renderHook(() => useBannerGroupState());
  act(() => registeredIds.forEach((id) => hook.result.current.contextValue.registerBanner(id)));

  return hook;
};

type HookResult = ReturnType<typeof renderState>["result"];

const hiddenIds = (result: HookResult, ids: string[]) =>
  ids.filter((id) => result.current.contextValue.checkIsBannerHidden(id));

describe("useBannerGroupState", () => {
  beforeEach(() => {
    window.localStorage.clear();
    mockUserId = "user-1";
  });

  describe("with nothing registered", () => {
    it("should count no hideable banners", () => {
      const { result } = renderState();

      expect(result.current.hideableCount).toBe(0);
    });

    it("should not show the toggle", () => {
      const { result } = renderState();

      expect(result.current.isToggleShown).toBe(false);
    });

    it("should start expanded", () => {
      const { result } = renderState();

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should not hide a banner", () => {
      const { result } = renderState();

      expect(hiddenIds(result, ["a"])).toEqual([]);
    });
  });

  describe("toggle visibility", () => {
    it("should not show the toggle for one registered banner", () => {
      const { result } = renderState(["a"]);

      expect(result.current.isToggleShown).toBe(false);
    });

    it("should show the toggle from two registered banners", () => {
      const { result } = renderState(["a", "b"]);

      expect(result.current.isToggleShown).toBe(true);
    });

    it("should count the registered banners", () => {
      const { result } = renderState(["a", "b"]);

      expect(result.current.hideableCount).toBe(2);
    });
  });

  describe("registry", () => {
    it("should ignore registering the same id twice", () => {
      const { result } = renderState(["a", "a", "a"]);

      expect(result.current.hideableCount).toBe(1);
    });

    it("should ignore unregistering an id that was never registered", () => {
      const { result } = renderState(["a"]);

      act(() => result.current.contextValue.unregisterBanner("missing"));

      expect(result.current.hideableCount).toBe(1);
    });

    it("should stop counting an unregistered banner", () => {
      const { result } = renderState(["a", "b", "c"]);

      act(() => result.current.contextValue.unregisterBanner("b"));

      expect(result.current.hideableCount).toBe(2);
    });

    describe("when the registry changes", () => {
      let initial: HookResult["current"]["contextValue"];
      let afterChange: HookResult["current"]["contextValue"];

      beforeEach(() => {
        const { result, rerender } = renderState(["a"]);
        initial = result.current.contextValue;

        act(() => result.current.contextValue.registerBanner("b"));
        rerender();
        afterChange = result.current.contextValue;
      });

      it("should keep registerBanner referentially stable, so banners do not re-register on every render", () => {
        expect(afterChange.registerBanner).toBe(initial.registerBanner);
      });

      it("should keep unregisterBanner referentially stable, so banners do not re-register on every render", () => {
        expect(afterChange.unregisterBanner).toBe(initial.unregisterBanner);
      });
    });
  });

  describe("when the group is not persisted as collapsed", () => {
    it("should not report the group as collapsed", () => {
      const { result } = renderState(["a", "b"]);

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should hide nothing", () => {
      const { result } = renderState(["a", "b"]);

      expect(hiddenIds(result, ["a", "b"])).toEqual([]);
    });
  });

  describe("when collapsed is persisted and every registered banner has been seen", () => {
    beforeEach(() => persistCollapsed("a", "b"));

    it("should report the group as collapsed", () => {
      const { result } = renderState(["a", "b"]);

      expect(result.current.isCollapsed).toBe(true);
    });

    it("should hide every banner", () => {
      const { result } = renderState(["a", "b"]);

      expect(hiddenIds(result, ["a", "b"])).toEqual(["a", "b"]);
    });

    it("should judge a banner that has not registered yet as if it already had, so it does not flash", () => {
      const { result } = renderState(["a"]);

      expect(hiddenIds(result, ["b"])).toEqual(["b"]);
    });
  });

  describe("when collapsed is persisted and a registered banner has not been seen", () => {
    beforeEach(() => persistCollapsed("a"));

    it("should not report the group as collapsed", () => {
      const { result } = renderState(["a", "b"]);

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should hide nothing", () => {
      const { result } = renderState(["a", "b"]);

      expect(hiddenIds(result, ["a", "b"])).toEqual([]);
    });

    it("should not hide a banner that has not registered yet when it was never seen", () => {
      const { result } = renderState(["a"]);

      expect(hiddenIds(result, ["new"])).toEqual([]);
    });
  });

  describe("with a lone banner and collapsed persisted", () => {
    beforeEach(() => persistCollapsed("a"));

    it("should not show the toggle", () => {
      const { result } = renderState(["a"]);

      expect(result.current.isToggleShown).toBe(false);
    });

    it("should not report the group as collapsed", () => {
      const { result } = renderState(["a"]);

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should never hide the banner", () => {
      const { result } = renderState(["a"]);

      expect(hiddenIds(result, ["a"])).toEqual([]);
    });
  });

  describe("when the count drops from 2 to 1 while collapsed", () => {
    beforeEach(() => persistCollapsed("a", "b"));

    it("should no longer report the group as collapsed", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.contextValue.unregisterBanner("b"));

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should stop hiding the remaining banner", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.contextValue.unregisterBanner("b"));

      expect(hiddenIds(result, ["a"])).toEqual([]);
    });
  });

  describe("when an unseen banner registers after the others while collapsed", () => {
    beforeEach(() => persistCollapsed("a", "b"));

    it("should no longer report the group as collapsed", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.contextValue.registerBanner("c"));

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should stop hiding the banners", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.contextValue.registerBanner("c"));

      expect(hiddenIds(result, ["a", "b", "c"])).toEqual([]);
    });
  });

  describe("when a seen banner registers after the others while collapsed", () => {
    beforeEach(() => persistCollapsed("a", "b", "c"));

    it("should keep the group collapsed", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.contextValue.registerBanner("c"));

      expect(result.current.isCollapsed).toBe(true);
    });

    it("should hide every banner", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.contextValue.registerBanner("c"));

      expect(hiddenIds(result, ["a", "b", "c"])).toEqual(["a", "b", "c"]);
    });
  });

  describe("collapseBannerGroup", () => {
    it("should collapse the group", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.collapseBannerGroup());

      expect(result.current.isCollapsed).toBe(true);
    });

    it("should persist the collapsed state with every registered banner as seen", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.collapseBannerGroup());

      expect(readPersisted()["user-1"]).toEqual({ isCollapsed: true, seenIds: ["a", "b"] });
    });

    it("should never forget banners that are not registered right now", () => {
      persist({ "user-1": { isCollapsed: false, seenIds: ["gone"] } });
      const { result } = renderState(["a", "b"]);

      act(() => result.current.collapseBannerGroup());

      expect(readPersisted()["user-1"].seenIds).toEqual(["gone", "a", "b"]);
    });

    describe("when the group was forced open by an unseen banner", () => {
      beforeEach(() => persistCollapsed("a", "b"));

      it("should start out expanded", () => {
        const { result } = renderState(["a", "b", "c"]);

        expect(result.current.isCollapsed).toBe(false);
      });

      it("should collapse the group", () => {
        const { result } = renderState(["a", "b", "c"]);

        act(() => result.current.collapseBannerGroup());

        expect(result.current.isCollapsed).toBe(true);
      });

      it("should mark the unseen banner as seen", () => {
        const { result } = renderState(["a", "b", "c"]);

        act(() => result.current.collapseBannerGroup());

        expect(readPersisted()["user-1"]).toEqual({ isCollapsed: true, seenIds: ["a", "b", "c"] });
      });
    });
  });

  describe("expandBannerGroup", () => {
    beforeEach(() => persistCollapsed("a", "b"));

    it("should expand the group", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.expandBannerGroup());

      expect(result.current.isCollapsed).toBe(false);
    });

    it("should persist the expanded state and keep the banners seen", () => {
      const { result } = renderState(["a", "b"]);

      act(() => result.current.expandBannerGroup());

      expect(readPersisted()["user-1"]).toEqual({ isCollapsed: false, seenIds: ["a", "b"] });
    });
  });

  describe("per-user state", () => {
    describe("for a user with no persisted state", () => {
      beforeEach(() => {
        persistCollapsed("a", "b");
        mockUserId = "user-2";
      });

      it("should not report the group as collapsed", () => {
        const { result } = renderState(["a", "b"]);

        expect(result.current.isCollapsed).toBe(false);
      });

      it("should hide nothing", () => {
        const { result } = renderState(["a", "b"]);

        expect(hiddenIds(result, ["a", "b"])).toEqual([]);
      });
    });

    it("should keep the other users' state when collapsing", () => {
      persist({ "user-1": { isCollapsed: true, seenIds: ["x", "y"] } });
      mockUserId = "user-2";
      const { result } = renderState(["a", "b"]);

      act(() => result.current.collapseBannerGroup());

      expect(readPersisted()).toEqual({
        "user-1": { isCollapsed: true, seenIds: ["x", "y"] },
        "user-2": { isCollapsed: true, seenIds: ["a", "b"] },
      });
    });
  });
});
