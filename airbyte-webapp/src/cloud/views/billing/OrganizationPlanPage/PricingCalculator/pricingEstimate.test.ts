import {
  calculatePlusQuote,
  calculateStandardPrice,
  clampCredits,
  estimatePricing,
  MAX_CREDITS,
  MIN_CREDITS,
} from "./pricingEstimate";

describe("calculateStandardPrice", () => {
  it.each([
    [0, 20],
    [5, 20],
    [6, 25],
    [1000, 4995],
    [2000, 9995],
  ])("prices %i credits on Standard at $%i", (credits, price) => {
    expect(calculateStandardPrice(credits)).toBe(price);
  });
});

describe("calculatePlusQuote", () => {
  it.each([
    [0, 40, 189],
    [5, 40, 189],
    [6, 40, 189],
    [39, 40, 189],
    [60, 40, 289],
    [92, 100, 449],
    [100, 100, 449],
    [750, 500, 2836.5],
    [1000, 1000, 3199],
    [1480, 2000, 4999],
    [1720, 2000, 4999],
    [2000, 2000, 4999],
  ])("prices %i credits on Plus %i at $%s", (credits, tierCredits, price) => {
    expect(calculatePlusQuote(credits)).toEqual({ tierCredits, price });
  });

  it("prefers the higher tier when a lower tier plus overage costs the same", () => {
    expect(calculatePlusQuote(92)).toEqual({ tierCredits: 100, price: 449 });
    expect(calculatePlusQuote(1480)).toEqual({ tierCredits: 2000, price: 4999 });
  });
});

describe("estimatePricing", () => {
  it("recommends Standard for every credit count from 0 through 38", () => {
    for (let credits = 0; credits <= 38; credits++) {
      expect(estimatePricing(credits).recommendation).toEqual({ plan: "standard" });
      expect(estimatePricing(credits).cheaperPlan).toBe("standard");
    }
  });

  it("recommends a Plus tier for every credit count from 39 through 1999", () => {
    for (let credits = 39; credits < MAX_CREDITS; credits++) {
      expect(estimatePricing(credits).recommendation.plan).toBe("plus");
      expect(estimatePricing(credits).cheaperPlan).toBe("plus");
    }
  });

  it.each([
    [39, 40],
    [60, 40],
    [92, 100],
    [750, 500],
    [1720, 2000],
  ])("names the cheapest Plus tier for %i credits", (credits, tierCredits) => {
    expect(estimatePricing(credits).recommendation).toEqual({ plan: "plus", tierCredits });
  });

  it("recommends Pro at the 2000 credit ceiling while still quoting Plus", () => {
    const estimate = estimatePricing(2000);
    expect(estimate.recommendation).toEqual({ plan: "pro" });
    expect(estimate.plus).toEqual({ tierCredits: 2000, price: 4999 });
    expect(estimate.standardPrice).toBe(9995);
    expect(estimate.cheaperPlan).toBe("plus");
  });

  it.each([
    [0, 0, 0],
    [5, 830_000, 1250],
    [1000, 166_000_000, 250_000],
    [2000, 332_000_000, 500_000],
  ])("converts %i credits into %i rows and %i megabytes", (credits, rows, megabytes) => {
    expect(estimatePricing(credits)).toEqual(expect.objectContaining({ rows, megabytes }));
  });
});

describe("clampCredits", () => {
  it.each([
    [-5, MIN_CREDITS],
    [0, 0],
    [10.6, 11],
    [2000, MAX_CREDITS],
    [2500, MAX_CREDITS],
  ])("clamps %s to %i", (input, expected) => {
    expect(clampCredits(input)).toBe(expected);
  });
});
