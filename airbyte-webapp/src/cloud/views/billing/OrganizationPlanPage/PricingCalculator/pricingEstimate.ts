import { PLUS_PLAN_TIERS } from "../PlanGrid/PlusPlanGridCard";

export const MIN_CREDITS = 0;
export const MAX_CREDITS = 2000;

const STANDARD_BASE_PRICE = 20;
const STANDARD_INCLUDED_CREDITS = 5;
const STANDARD_RATE_PER_CREDIT = 5;

export const ROWS_PER_CREDIT = 166_000;
export const MEGABYTES_PER_CREDIT = 250;

export type SelfServePlan = "standard" | "plus";

export type PlanRecommendation = { plan: "standard" } | { plan: "plus"; tierCredits: number } | { plan: "pro" };

export interface PlusQuote {
  /** Credits included in the tier that produces the cheapest price. */
  tierCredits: number;
  price: number;
}

export interface PricingEstimate {
  credits: number;
  standardPrice: number;
  plus: PlusQuote;
  /** Cheaper of the two self-serve plans. A tie goes to Plus. */
  cheaperPlan: SelfServePlan;
  recommendation: PlanRecommendation;
  rows: number;
  megabytes: number;
}

export const clampCredits = (credits: number): number =>
  Math.min(MAX_CREDITS, Math.max(MIN_CREDITS, Math.round(credits)));

const toCents = (dollars: number): number => Math.round(dollars * 100);

/** $20 covers the first 5 credits, then $5 per credit. */
export const calculateStandardPrice = (credits: number): number =>
  STANDARD_BASE_PRICE + STANDARD_RATE_PER_CREDIT * Math.max(0, credits - STANDARD_INCLUDED_CREDITS);

/**
 * Cheapest way to buy `credits` on Plus: either a lower tier plus overage or a higher tier left partly unused.
 * Prices are compared in whole cents so ties resolve exactly, and a tie goes to the higher tier.
 */
export const calculatePlusQuote = (credits: number): PlusQuote => {
  let best = { tierCredits: 0, priceInCents: Number.POSITIVE_INFINITY };
  for (const tier of PLUS_PLAN_TIERS) {
    const priceInCents = toCents(tier.monthlyPrice) + toCents(tier.overageRate) * Math.max(0, credits - tier.credits);
    if (priceInCents < best.priceInCents || (priceInCents === best.priceInCents && tier.credits > best.tierCredits)) {
      best = { tierCredits: tier.credits, priceInCents };
    }
  }
  return { tierCredits: best.tierCredits, price: best.priceInCents / 100 };
};

export const estimatePricing = (credits: number): PricingEstimate => {
  const standardPrice = calculateStandardPrice(credits);
  const plus = calculatePlusQuote(credits);
  const cheaperPlan: SelfServePlan = standardPrice < plus.price ? "standard" : "plus";
  const recommendation: PlanRecommendation =
    credits >= MAX_CREDITS
      ? { plan: "pro" }
      : cheaperPlan === "standard"
      ? { plan: "standard" }
      : { plan: "plus", tierCredits: plus.tierCredits };

  return {
    credits,
    standardPrice,
    plus,
    cheaperPlan,
    recommendation,
    rows: credits * ROWS_PER_CREDIT,
    megabytes: credits * MEGABYTES_PER_CREDIT,
  };
};
