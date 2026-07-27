export interface WeightEntry {
  date: string;
  weightKg: number;
}

export interface DailyLog {
  waterMl: number;
  proteinHit: boolean | null;
  strength: boolean;
}

export type SideEffectTag =
  | "Kvalme"
  | "Forstoppelse"
  | "Diarré"
  | "Hovedpine"
  | "Halsbrand/Syre"
  | "Træthed"
  | "Rådne æg-bøvs";

export interface SideEffectEntry {
  id: string;
  date: string;
  tag: SideEffectTag;
  severity: 1 | 2 | 3 | 4 | 5;
}

export interface AppState {
  setupComplete: boolean;
  startDate: string;
  startWeight: number;
  goalWeight: number;
  injectionDay: number;
  pausedDaysAccumulated: number;
  lockStartedAt: string | null;
  weightLogs: WeightEntry[];
  dailyLogs: Record<string, DailyLog>;
  sideEffectLogs: SideEffectEntry[];
  injectionLog: string[];
}

export const SIDE_EFFECT_TAGS: SideEffectTag[] = [
  "Kvalme",
  "Forstoppelse",
  "Diarré",
  "Hovedpine",
  "Halsbrand/Syre",
  "Træthed",
  "Rådne æg-bøvs",
];
