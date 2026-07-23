import styles from "./CreditCardLogo.module.scss";
import Amex from "./logos/amex.svg?react";
import Diners from "./logos/diners.svg?react";
import Discover from "./logos/discover.svg?react";
import Generic from "./logos/generic.svg?react";
import Maestro from "./logos/maestro.svg?react";
import Mastercard from "./logos/mastercard.svg?react";
import UnionPay from "./logos/unionpay.svg?react";
import Visa from "./logos/visa.svg?react";

interface BrandProps {
  brand?: string;
}

export const CreditCardLogo: React.FC<BrandProps> = ({ brand }) => {
  return (
    <div className={styles.creditCardLogo}>
      <Logo brand={brand} />
    </div>
  );
};

export const Logo: React.FC<BrandProps> = ({ brand }) => {
  switch (brand) {
    case "visa":
      return <Visa />;
    case "mastercard":
      return <Mastercard />;
    case "amex":
      return <Amex />;
    case "diners":
      return <Diners />;
    case "discover":
      return <Discover />;
    case "maestro":
      return <Maestro />;
    case "unionpay":
      return <UnionPay />;
    default:
      return <Generic />;
  }
};
