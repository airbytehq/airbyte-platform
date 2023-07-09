import { useTooltipContext } from "./context";
import styles from "./TooltipTable.module.scss";

interface TooltipTableProps {
  rows: React.ReactNode[][];
}

export const TooltipTable: React.FC<TooltipTableProps> = ({ rows }) => {
  const { theme } = useTooltipContext();

  return rows.length > 0 ? (
    <table className={theme === "light" ? styles.light : undefined}>
      <tbody>
        {rows?.map((cols, rowIndex) => (
          <tr key={rowIndex}>
            {cols.map((col, colIndex) => (
              <td key={colIndex} className={colIndex === 0 ? styles.label : undefined}>
                {col}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  ) : null;
};
