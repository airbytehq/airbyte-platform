import classNames from "classnames";
import kebabCase from "lodash/kebabCase";
import React from "react";

import styles from "./Icon.module.scss";
import AddCircleIcon from "./icons/addCircleIcon.svg?react";
import ArrowLeftIcon from "./icons/arrowLeftIcon.svg?react";
import ArrowRightIcon from "./icons/arrowRightIcon.svg?react";
import ArticleIcon from "./icons/articleIcon.svg?react";
import BellIcon from "./icons/bellIcon.svg?react";
import CalendarCheckIcon from "./icons/calendarCheckIcon.svg?react";
import CalendarIcon from "./icons/calendarIcon.svg?react";
import CaretDownIcon from "./icons/caretDownIcon.svg?react";
import CastIcon from "./icons/castIcon.svg?react";
import CertifiedIcon from "./icons/certifiedIcon.svg?react";
import ChartIcon from "./icons/chartIcon.svg?react";
import ChatIcon from "./icons/chatIcon.svg?react";
import CheckIcon from "./icons/checkIcon.svg?react";
import ChevronDownIcon from "./icons/chevronDownIcon.svg?react";
import ChevronLeftIcon from "./icons/chevronLeftIcon.svg?react";
import ChevronRightIcon from "./icons/chevronRightIcon.svg?react";
import ChevronUpIcon from "./icons/chevronUpIcon.svg?react";
import ClockFilledIcon from "./icons/clockFilledIcon.svg?react";
import ClockOutlineIcon from "./icons/clockOutlineIcon.svg?react";
import CommentsIcon from "./icons/commentsIcon.svg?react";
import CommunityIcon from "./icons/communityIcon.svg?react";
import ConnectionIcon from "./icons/connectionIcon.svg?react";
import ContractIcon from "./icons/contractIcon.svg?react";
import CopyIcon from "./icons/copyIcon.svg?react";
import CreditsIcon from "./icons/creditsIcon.svg?react";
import CrossIcon from "./icons/crossIcon.svg?react";
import DatabaseIcon from "./icons/databaseIcon.svg?react";
import DayIcon from "./icons/dayIcon.svg?react";
import DbtCloudIcon from "./icons/dbtCloudIcon.svg?react";
import DestinationIcon from "./icons/destinationIcon.svg?react";
import DisabledIcon from "./icons/disabledIcon.svg?react";
import DockerIcon from "./icons/dockerIcon.svg?react";
import DocsIcon from "./icons/docsIcon.svg?react";
import DownloadIcon from "./icons/downloadIcon.svg?react";
import DuplicateIcon from "./icons/duplicateIcon.svg?react";
import EarthIcon from "./icons/earthIcon.svg?react";
import EqualIcon from "./icons/equalIcon.svg?react";
import ErrorFilledIcon from "./icons/errorFilledIcon.svg?react";
import ErrorOutlineIcon from "./icons/errorOutlineIcon.svg?react";
import ExpandIcon from "./icons/expandIcon.svg?react";
import EyeIcon from "./icons/eyeIcon.svg?react";
import EyeSlashIcon from "./icons/eyeSlashIcon.svg?react";
import FileIcon from "./icons/fileIcon.svg?react";
import FilesIcon from "./icons/filesIcon.svg?react";
import FlashIcon from "./icons/flashIcon.svg?react";
import FolderIcon from "./icons/folderIcon.svg?react";
import GearIcon from "./icons/gearIcon.svg?react";
import GlobeIcon from "./icons/globeIcon.svg?react";
import GoogleIcon from "./icons/googleIcon.svg?react";
import GridIcon from "./icons/gridIcon.svg?react";
import HelpIcon from "./icons/helpIcon.svg?react";
import HouseIcon from "./icons/houseIcon.svg?react";
import IdCardIcon from "./icons/idCardIcon.svg?react";
import ImportIcon from "./icons/importIcon.svg?react";
import InfoFilledIcon from "./icons/infoFilledIcon.svg?react";
import InfoOutlineIcon from "./icons/infoOutlineIcon.svg?react";
import IntegrationsIcon from "./icons/integrationsIcon.svg?react";
import KeyCircleIcon from "./icons/keyCircleIcon.svg?react";
import LayersIcon from "./icons/layersIcon.svg?react";
import LensIcon from "./icons/lensIcon.svg?react";
import LightbulbIcon from "./icons/lightbulbIcon.svg?react";
import LinkIcon from "./icons/linkIcon.svg?react";
import LoadingIcon from "./icons/loadingIcon.svg?react";
import LocationIcon from "./icons/locationIcon.svg?react";
import LockIcon from "./icons/lockIcon.svg?react";
import MenuIcon from "./icons/menuIcon.svg?react";
import MinusCircleIcon from "./icons/minusCircleIcon.svg?react";
import MinusIcon from "./icons/minusIcon.svg?react";
import ModificationIcon from "./icons/modificationIcon.svg?react";
import MonitorIcon from "./icons/monitorIcon.svg?react";
import MoonIcon from "./icons/moonIcon.svg?react";
import NestedIcon from "./icons/nestedIcon.svg?react";
import NoteIcon from "./icons/noteIcon.svg?react";
import NotificationIcon from "./icons/notificationIcon.svg?react";
import OnboardingIcon from "./icons/onboardingIcon.svg?react";
import OptionsIcon from "./icons/optionsIcon.svg?react";
import ParametersIcon from "./icons/parametersIcon.svg?react";
import PauseFilledIcon from "./icons/pauseFilledIcon.svg?react";
import PauseOutlineIcon from "./icons/pauseOutlineIcon.svg?react";
import PencilIcon from "./icons/pencilIcon.svg?react";
import PlayIcon from "./icons/playIcon.svg?react";
import PlusIcon from "./icons/plusIcon.svg?react";
import PrefixIcon from "./icons/prefixIcon.svg?react";
import PulseIcon from "./icons/pulseIcon.svg?react";
import QuestionIcon from "./icons/questionIcon.svg?react";
import RecipesIcon from "./icons/recipesIcon.svg?react";
import ResetIcon from "./icons/resetIcon.svg?react";
import RocketIcon from "./icons/rocketIcon.svg?react";
import RotateIcon from "./icons/rotateIcon.svg?react";
import SchemaIcon from "./icons/schemaIcon.svg?react";
import SelectIcon from "./icons/selectIcon.svg?react";
import ShareIcon from "./icons/shareIcon.svg?react";
import ShortVideoIcon from "./icons/shortVideoIcon.svg?react";
import ShrinkIcon from "./icons/shrinkIcon.svg?react";
import SimpleCircleIcon from "./icons/simpleCircleIcon.svg?react";
import SlackIcon from "./icons/slackIcon.svg?react";
import SleepIcon from "./icons/sleepIcon.svg?react";
import SourceIcon from "./icons/sourceIcon.svg?react";
import StarIcon from "./icons/starIcon.svg?react";
import StarsIcon from "./icons/starsIcon.svg?react";
import StatusCancelledIcon from "./icons/statusCancelledIcon.svg?react";
import StatusErrorIcon from "./icons/statusErrorIcon.svg?react";
import StatusInactiveIcon from "./icons/statusInactiveIcon.svg?react";
import StatusInProgressIcon from "./icons/statusInProgressIcon.svg?react";
import StatusSleepIcon from "./icons/statusSleepIcon.svg?react";
import StatusSuccessIcon from "./icons/statusSuccessIcon.svg?react";
import StatusWarningIcon from "./icons/statusWarningIcon.svg?react";
import StopFilledIcon from "./icons/stopFilledIcon.svg?react";
import StopOutlineIcon from "./icons/stopOutlineIcon.svg?react";
import SuccessFilledIcon from "./icons/successFilledIcon.svg?react";
import SuccessOutlineIcon from "./icons/successOutlineIcon.svg?react";
import SuitcaseIcon from "./icons/suitcaseIcon.svg?react";
import SyncIcon from "./icons/syncIcon.svg?react";
import TableIcon from "./icons/tableIcon.svg?react";
import TargetIcon from "./icons/targetIcon.svg?react";
import TicketIcon from "./icons/ticketIcon.svg?react";
import TrashIcon from "./icons/trashIcon.svg?react";
import UnlockIcon from "./icons/unlockIcon.svg?react";
import UserIcon from "./icons/userIcon.svg?react";
import WarningFilledIcon from "./icons/warningFilledIcon.svg?react";
import WarningOutlineIcon from "./icons/warningOutlineIcon.svg?react";
import WrenchIcon from "./icons/wrenchIcon.svg?react";
import { IconColor, IconProps, IconType } from "./types";

const colorMap: Record<IconColor, string> = {
  warning: styles[`icon--warning`],
  success: styles[`icon--success`],
  primary: styles[`icon--primary`],
  disabled: styles[`icon--disabled`],
  error: styles[`icon--error`],
  action: styles[`icon--action`],
  affordance: styles[`icon--affordance`],
};

const sizeMap: Record<NonNullable<IconProps["size"]>, string> = {
  xs: styles.xs,
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
  xl: styles.xl,
};

export const Icons: Record<IconType, React.FC<React.SVGProps<SVGSVGElement>>> = {
  addCircle: AddCircleIcon,
  arrowLeft: ArrowLeftIcon,
  arrowRight: ArrowRightIcon,
  article: ArticleIcon,
  bell: BellIcon,
  calendarCheck: CalendarCheckIcon,
  calendar: CalendarIcon,
  caretDown: CaretDownIcon,
  cast: CastIcon,
  certified: CertifiedIcon,
  chart: ChartIcon,
  chat: ChatIcon,
  check: CheckIcon,
  chevronDown: ChevronDownIcon,
  chevronLeft: ChevronLeftIcon,
  chevronRight: ChevronRightIcon,
  chevronUp: ChevronUpIcon,
  clockFilled: ClockFilledIcon,
  clockOutline: ClockOutlineIcon,
  comments: CommentsIcon,
  community: CommunityIcon,
  connection: ConnectionIcon,
  contract: ContractIcon,
  copy: CopyIcon,
  credits: CreditsIcon,
  cross: CrossIcon,
  database: DatabaseIcon,
  day: DayIcon,
  dbtCloud: DbtCloudIcon,
  destination: DestinationIcon,
  disabled: DisabledIcon,
  docker: DockerIcon,
  docs: DocsIcon,
  download: DownloadIcon,
  duplicate: DuplicateIcon,
  earth: EarthIcon,
  equal: EqualIcon,
  errorFilled: ErrorFilledIcon,
  errorOutline: ErrorOutlineIcon,
  expand: ExpandIcon,
  eye: EyeIcon,
  eyeSlash: EyeSlashIcon,
  file: FileIcon,
  files: FilesIcon,
  flash: FlashIcon,
  folder: FolderIcon,
  gear: GearIcon,
  globe: GlobeIcon,
  google: GoogleIcon,
  grid: GridIcon,
  help: HelpIcon,
  house: HouseIcon,
  idCard: IdCardIcon,
  import: ImportIcon,
  infoFilled: InfoFilledIcon,
  infoOutline: InfoOutlineIcon,
  integrations: IntegrationsIcon,
  keyCircle: KeyCircleIcon,
  layers: LayersIcon,
  lens: LensIcon,
  lightbulb: LightbulbIcon,
  link: LinkIcon,
  loading: LoadingIcon,
  location: LocationIcon,
  lock: LockIcon,
  menu: MenuIcon,
  minusCircle: MinusCircleIcon,
  minus: MinusIcon,
  modification: ModificationIcon,
  monitor: MonitorIcon,
  moon: MoonIcon,
  nested: NestedIcon,
  note: NoteIcon,
  notification: NotificationIcon,
  onboarding: OnboardingIcon,
  options: OptionsIcon,
  parameters: ParametersIcon,
  pauseFilled: PauseFilledIcon,
  pauseOutline: PauseOutlineIcon,
  pencil: PencilIcon,
  play: PlayIcon,
  plus: PlusIcon,
  prefix: PrefixIcon,
  pulse: PulseIcon,
  question: QuestionIcon,
  recipes: RecipesIcon,
  reset: ResetIcon,
  rocket: RocketIcon,
  rotate: RotateIcon,
  schema: SchemaIcon,
  select: SelectIcon,
  share: ShareIcon,
  shortVideo: ShortVideoIcon,
  shrink: ShrinkIcon,
  simpleCircle: SimpleCircleIcon,
  slack: SlackIcon,
  sleep: SleepIcon,
  source: SourceIcon,
  star: StarIcon,
  stars: StarsIcon,
  statusCancelled: StatusCancelledIcon,
  statusError: StatusErrorIcon,
  statusInactive: StatusInactiveIcon,
  statusInProgress: StatusInProgressIcon,
  statusSleep: StatusSleepIcon,
  statusSuccess: StatusSuccessIcon,
  statusWarning: StatusWarningIcon,
  stopFilled: StopFilledIcon,
  stopOutline: StopOutlineIcon,
  successFilled: SuccessFilledIcon,
  successOutline: SuccessOutlineIcon,
  suitcase: SuitcaseIcon,
  sync: SyncIcon,
  table: TableIcon,
  target: TargetIcon,
  ticket: TicketIcon,
  trash: TrashIcon,
  unlock: UnlockIcon,
  user: UserIcon,
  warningFilled: WarningFilledIcon,
  warningOutline: WarningOutlineIcon,
  wrench: WrenchIcon,
};

export const Icon: React.FC<IconProps> = React.memo(
  ({ type, color, size = "md", withBackground, className, ...props }) => {
    const classes = classNames(
      className,
      styles.icon,
      color ? colorMap[color] : undefined,
      withBackground ? styles["icon--withBackground"] : undefined,
      type === "loading" ? styles["icon--spinning"] : undefined,
      sizeMap[size]
    );

    return React.createElement(Icons[type], {
      ...props,
      // @ts-expect-error data-* attributes aren't allowed outside a JSX tag
      "data-icon": kebabCase(type),
      className: classes,
    });
  }
);
Icon.displayName = "Icon";
