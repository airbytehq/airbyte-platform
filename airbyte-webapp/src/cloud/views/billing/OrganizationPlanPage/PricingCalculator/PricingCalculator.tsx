import classNames from "classnames";
import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";
import { useDebounceValue } from "core/utils/useDebounceValue";

import styles from "./PricingCalculator.module.scss";
import { clampCredits, estimatePricing, MAX_CREDITS, MIN_CREDITS } from "./pricingEstimate";

const DEFAULT_CREDITS = 5;
const DEBOUNCE_MS = 300;
const MEGABYTES_PER_GIGABYTE = 1000;
const CREDITS_LABEL_ID = "pricing-calculator-credits-label";

export const PricingCalculator: React.FC = () => {
  const { formatNumber } = useIntl();
  const [credits, setCredits] = useState(DEFAULT_CREDITS);
  const [creditsInput, setCreditsInput] = useState(String(DEFAULT_CREDITS));
  const estimate = estimatePricing(useDebounceValue(credits, DEBOUNCE_MS));

  const onSliderChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const next = clampCredits(Number(event.target.value));
    setCredits(next);
    setCreditsInput(String(next));
  };

  const onInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const parsed = Number.parseInt(event.target.value, 10);
    if (Number.isNaN(parsed)) {
      setCreditsInput(event.target.value);
      return;
    }
    const clamped = clampCredits(parsed);
    setCredits(clamped);
    setCreditsInput(clamped === parsed ? event.target.value : String(clamped));
  };

  const formatPrice = (price: number) =>
    formatNumber(price, {
      style: "currency",
      currency: "USD",
      minimumFractionDigits: Number.isInteger(price) ? 0 : 2,
      maximumFractionDigits: 2,
    });

  const showGigabytes = estimate.megabytes >= MEGABYTES_PER_GIGABYTE;
  const sliderFill = `${((credits - MIN_CREDITS) / (MAX_CREDITS - MIN_CREDITS)) * 100}%`;
  const { recommendation } = estimate;

  return (
    <FlexContainer direction="column" gap="lg" data-testid="pricing-calculator">
      <FlexContainer direction="column" gap="sm">
        <Heading as="h2" size="sm">
          <FormattedMessage id="planGrid.pricingCalculator.title" />
        </Heading>
        <Text size="lg" color="grey400">
          <FormattedMessage id="planGrid.pricingCalculator.description" />
        </Text>
      </FlexContainer>
      <div className={styles.calculator}>
        <Text bold id={CREDITS_LABEL_ID}>
          <FormattedMessage id="planGrid.pricingCalculator.monthlyCredits" />
        </Text>
        <input
          type="range"
          className={styles.slider}
          min={MIN_CREDITS}
          max={MAX_CREDITS}
          step={1}
          value={credits}
          onChange={onSliderChange}
          aria-labelledby={CREDITS_LABEL_ID}
          style={{ "--slider-fill": sliderFill } as React.CSSProperties}
        />
        <div className={styles.creditsInput}>
          <input
            type="number"
            className={styles.creditsInput__field}
            min={MIN_CREDITS}
            max={MAX_CREDITS}
            value={creditsInput}
            onChange={onInputChange}
            onBlur={() => setCreditsInput(String(credits))}
            aria-labelledby={CREDITS_LABEL_ID}
          />
          <span className={styles.creditsInput__divider} />
          <Text size="lg" color="grey500">
            <FormattedMessage id="planGrid.pricingCalculator.monthlyCredits" />
          </Text>
        </div>
        <div className={styles.tiles}>
          <StatTile
            data-testid="pricing-calculator-standard-price"
            highlighted={estimate.cheaperPlan === "standard"}
            title={<FormattedMessage id="planGrid.pricingCalculator.standardPrice" />}
            value={formatPrice(estimate.standardPrice)}
            unit={<FormattedMessage id="planGrid.pricingCalculator.perMonth" />}
            footnote={<FormattedMessage id="planGrid.pricingCalculator.billedMonthly" />}
          />
          <StatTile
            data-testid="pricing-calculator-plus-price"
            highlighted={estimate.cheaperPlan === "plus"}
            title={<FormattedMessage id="planGrid.pricingCalculator.plusPrice" />}
            value={formatPrice(estimate.plus.price)}
            unit={<FormattedMessage id="planGrid.pricingCalculator.perMonth" />}
            footnote={<FormattedMessage id="planGrid.pricingCalculator.billedMonthly" />}
          />
          <StatTile
            data-testid="pricing-calculator-rows"
            title={<FormattedMessage id="planGrid.pricingCalculator.apis" />}
            value={formatNumber(estimate.rows, { notation: "compact", maximumFractionDigits: 1 })}
            unit={<FormattedMessage id="planGrid.pricingCalculator.rowsPerMonth" />}
            footnote={<FormattedMessage id="planGrid.pricingCalculator.rowsPerCredit" />}
          />
          <StatTile
            data-testid="pricing-calculator-data-volume"
            title={<FormattedMessage id="planGrid.pricingCalculator.databases" />}
            value={
              showGigabytes
                ? formatNumber(estimate.megabytes / MEGABYTES_PER_GIGABYTE, { maximumFractionDigits: 0 })
                : formatNumber(estimate.megabytes)
            }
            unit={
              <FormattedMessage
                id={
                  showGigabytes
                    ? "planGrid.pricingCalculator.gigabytesPerMonth"
                    : "planGrid.pricingCalculator.megabytesPerMonth"
                }
              />
            }
            footnote={<FormattedMessage id="planGrid.pricingCalculator.megabytesPerCredit" />}
          />
        </div>
        <div
          className={classNames(styles.recommendation, {
            [styles["recommendation--pro"]]: recommendation.plan === "pro",
          })}
          data-testid="pricing-calculator-recommendation"
        >
          <FlexContainer alignItems="center" gap="sm">
            <Icon type="aiStars" size="md" />
            <span className={styles.recommendation__title}>
              <FormattedMessage id="planGrid.pricingCalculator.costOptimization" />
            </span>
          </FlexContainer>
          <p className={styles.recommendation__text}>
            {recommendation.plan === "standard" ? (
              <FormattedMessage id="planGrid.pricingCalculator.recommendation.standard" />
            ) : recommendation.plan === "plus" ? (
              <FormattedMessage
                id="planGrid.pricingCalculator.recommendation.plus"
                values={{ credits: recommendation.tierCredits }}
              />
            ) : (
              <FormattedMessage
                id="planGrid.pricingCalculator.recommendation.pro"
                values={{
                  lnk: (node: React.ReactNode) => <ExternalLink href={links.contactSales}>{node}</ExternalLink>,
                }}
              />
            )}
          </p>
        </div>
      </div>
    </FlexContainer>
  );
};

interface StatTileProps {
  title: React.ReactNode;
  value: React.ReactNode;
  unit: React.ReactNode;
  footnote: React.ReactNode;
  highlighted?: boolean;
  "data-testid": string;
}

const StatTile: React.FC<StatTileProps> = ({
  title,
  value,
  unit,
  footnote,
  highlighted = false,
  "data-testid": testId,
}) => (
  <div
    className={classNames(styles.tile, { [styles["tile--highlighted"]]: highlighted })}
    data-testid={testId}
    data-highlighted={highlighted}
  >
    <Text size="sm" color="grey500" bold>
      {title}
    </Text>
    <FlexContainer alignItems="baseline" gap="xs">
      <span className={styles.tile__value}>{value}</span>
      <Text as="span" size="md" color="grey500" bold>
        {unit}
      </Text>
    </FlexContainer>
    <Text size="sm" color="grey400">
      {footnote}
    </Text>
  </div>
);
