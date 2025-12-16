import styles from "./UsageByWorkspaceGraph.module.scss";

const WORKSPACE_COLORS = [
  styles.blue800, // #300ad6 - Dark blue
  styles.pink500, // #e94ea2 - Bright pink
  styles.cyan700, // #008f99 - Teal
  styles.orange200, // #fea895 - Light coral
  styles.blue400, // #605cff - Blue-purple
  styles.pink700, // #b600b6 - Magenta
  styles.purple400, // #ac56e9 - Light purple
  styles.blueBright, // Bright blue
  styles.pink200, // #ff8fa3 - Light pink
  styles.cyan500, // #07becd - Cyan
] as const;

/**
 * Get the color for a workspace based on its index.
 */
export function getWorkspaceColorByIndex(index: number): string {
  return WORKSPACE_COLORS[index % WORKSPACE_COLORS.length];
}
