import { daysBetween, todayISO } from "./date";
import type { AppState } from "./types";

export interface DosePhase {
  index: number;
  doseLabel: string;
  penLabel: string;
  penColorClass: string;
  weekStart: number;
  weekEnd: number | null;
}

export const DOSE_PHASES: DosePhase[] = [
  { index: 0, doseLabel: "0,25 mg", penLabel: "Grøn pen", penColorClass: "bg-emerald-500", weekStart: 1, weekEnd: 4 },
  { index: 1, doseLabel: "0,5 mg", penLabel: "Rød/Lyserød pen", penColorClass: "bg-rose-500", weekStart: 5, weekEnd: 8 },
  { index: 2, doseLabel: "1,0 mg", penLabel: "Brun pen", penColorClass: "bg-amber-700", weekStart: 9, weekEnd: 12 },
  { index: 3, doseLabel: "1,7 mg", penLabel: "Blå pen", penColorClass: "bg-blue-500", weekStart: 13, weekEnd: 16 },
  { index: 4, doseLabel: "2,4 mg", penLabel: "Sort/Grå pen (vedligeholdelse)", penColorClass: "bg-slate-400", weekStart: 17, weekEnd: null },
];

export function phaseForWeek(week: number): DosePhase {
  const clamped = Math.max(1, week);
  return (
    DOSE_PHASES.find((p) => clamped >= p.weekStart && (p.weekEnd === null || clamped <= p.weekEnd)) ??
    DOSE_PHASES[DOSE_PHASES.length - 1]
  );
}

export function effectiveElapsedDays(state: AppState, asOfISO: string = todayISO()): number {
  const rawElapsed = daysBetween(state.startDate, asOfISO);
  const activeLockDays = state.lockStartedAt ? Math.max(0, daysBetween(state.lockStartedAt, asOfISO)) : 0;
  return Math.max(0, rawElapsed - state.pausedDaysAccumulated - activeLockDays);
}

export function calendarWeek(state: AppState, asOfISO: string = todayISO()): number {
  return Math.floor(effectiveElapsedDays(state, asOfISO) / 7) + 1;
}

export function currentPhase(state: AppState, asOfISO: string = todayISO()): DosePhase {
  return phaseForWeek(calendarWeek(state, asOfISO));
}

export function daysIntoCurrentWeek(state: AppState, asOfISO: string = todayISO()): number {
  return effectiveElapsedDays(state, asOfISO) % 7;
}
