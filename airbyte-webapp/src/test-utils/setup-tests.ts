import "@testing-library/jest-dom";
import "../dayjs-setup";

// jsdom doesn't mock `matchMedia`, which is required by react-slick
Object.defineProperty(window, "matchMedia", {
  writable: true,
  value: jest.fn().mockImplementation((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: jest.fn(), // deprecated
    removeListener: jest.fn(), // deprecated
    addEventListener: jest.fn(),
    removeEventListener: jest.fn(),
    dispatchEvent: jest.fn(),
  })),
});

// required for headless UI
global.ResizeObserver = jest.fn().mockImplementation(() => ({
  observe: jest.fn(),
  unobserve: jest.fn(),
  disconnect: jest.fn(),
}));

// retry failed tests when configured to (e.g. `test:ci`)
if (process.env.JEST_RETRIES) {
  jest.retryTimes(parseInt(process.env.JEST_RETRIES, 10));
}
