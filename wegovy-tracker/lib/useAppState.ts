"use client";

import { useCallback, useEffect, useState } from "react";
import { daysBetween, todayISO, weekdayOf } from "./date";
import type { AppState, DailyLog, SideEffectEntry, WeightEntry } from "./types";

const STORAGE_KEY = "wegovy-tracker-state-v1";

function defaultState(): AppState {
  const start = todayISO();
  return {
    setupComplete: false,
    startDate: start,
    startWeight: 117,
    goalWeight: 107,
    injectionDay: weekdayOf(start),
    pausedDaysAccumulated: 0,
    lockStartedAt: null,
    weightLogs: [],
    dailyLogs: {},
    sideEffectLogs: [],
    injectionLog: [],
  };
}

function loadState(): AppState {
  if (typeof window === "undefined") return defaultState();
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return defaultState();
    return { ...defaultState(), ...JSON.parse(raw) };
  } catch {
    return defaultState();
  }
}

export function useAppState() {
  const [state, setState] = useState<AppState>(defaultState);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    setState(loadState());
    setHydrated(true);
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }, [state, hydrated]);

  const update = useCallback((patch: Partial<AppState>) => {
    setState((s) => ({ ...s, ...patch }));
  }, []);

  const completeSetup = useCallback((startWeight: number, goalWeight: number, startDate: string, injectionDay: number) => {
    setState((s) => ({ ...s, setupComplete: true, startWeight, goalWeight, startDate, injectionDay }));
  }, []);

  const addWeightEntry = useCallback((entry: WeightEntry) => {
    setState((s) => {
      const withoutSameDay = s.weightLogs.filter((w) => w.date !== entry.date);
      return { ...s, weightLogs: [...withoutSameDay, entry].sort((a, b) => a.date.localeCompare(b.date)) };
    });
  }, []);

  const setDailyLog = useCallback((date: string, patch: Partial<DailyLog>) => {
    setState((s) => {
      const existing: DailyLog = s.dailyLogs[date] ?? { waterMl: 0, proteinHit: null, strength: false };
      return { ...s, dailyLogs: { ...s.dailyLogs, [date]: { ...existing, ...patch } } };
    });
  }, []);

  const addSideEffect = useCallback((entry: SideEffectEntry) => {
    setState((s) => ({ ...s, sideEffectLogs: [entry, ...s.sideEffectLogs] }));
  }, []);

  const removeSideEffect = useCallback((id: string) => {
    setState((s) => ({ ...s, sideEffectLogs: s.sideEffectLogs.filter((e) => e.id !== id) }));
  }, []);

  const lockDose = useCallback(() => {
    setState((s) => (s.lockStartedAt ? s : { ...s, lockStartedAt: todayISO() }));
  }, []);

  const unlockDose = useCallback(() => {
    setState((s) => {
      if (!s.lockStartedAt) return s;
      const pausedDays = daysBetween(s.lockStartedAt, todayISO());
      return { ...s, lockStartedAt: null, pausedDaysAccumulated: s.pausedDaysAccumulated + Math.max(0, pausedDays) };
    });
  }, []);

  const setInjectionDay = useCallback((day: number) => {
    setState((s) => ({ ...s, injectionDay: day }));
  }, []);

  const logInjection = useCallback((date: string) => {
    setState((s) => (s.injectionLog.includes(date) ? s : { ...s, injectionLog: [...s.injectionLog, date].sort() }));
  }, []);

  return {
    state,
    hydrated,
    update,
    completeSetup,
    addWeightEntry,
    setDailyLog,
    addSideEffect,
    removeSideEffect,
    lockDose,
    unlockDose,
    setInjectionDay,
    logInjection,
  };
}
