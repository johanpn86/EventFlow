"use client";

import { useState } from "react";
import { DailyChecklist } from "@/components/DailyChecklist";
import { DoseTimeline } from "@/components/DoseTimeline";
import { Header } from "@/components/Header";
import { InjectionCountdown } from "@/components/InjectionCountdown";
import { QuickReference } from "@/components/QuickReference";
import { SetupForm } from "@/components/SetupForm";
import { SideEffectLog } from "@/components/SideEffectLog";
import { TabNav, type TabKey } from "@/components/TabNav";
import { WeightChart } from "@/components/WeightChart";
import { WeightQuickInput } from "@/components/WeightQuickInput";
import { todayISO } from "@/lib/date";
import { useAppState } from "@/lib/useAppState";

export default function Home() {
  const {
    state,
    hydrated,
    completeSetup,
    addWeightEntry,
    setDailyLog,
    addSideEffect,
    removeSideEffect,
    lockDose,
    unlockDose,
    setInjectionDay,
    logInjection,
  } = useAppState();
  const [tab, setTab] = useState<TabKey>("dashboard");

  if (!hydrated) {
    return <div className="flex min-h-screen items-center justify-center text-slate-500">Indlæser…</div>;
  }

  if (!state.setupComplete) {
    return <SetupForm onComplete={completeSetup} />;
  }

  const today = todayISO();
  const todayLog = state.dailyLogs[today] ?? { waterMl: 0, proteinHit: null, strength: false };
  const todayWeight = state.weightLogs.find((w) => w.date === today);

  return (
    <div className="pb-20">
      <Header state={state} />
      <main className="mx-auto flex max-w-md flex-col gap-4 p-4">
        {tab === "dashboard" && (
          <>
            <DoseTimeline state={state} onLock={lockDose} onUnlock={unlockDose} />
            <InjectionCountdown state={state} onChangeDay={setInjectionDay} onLogInjection={logInjection} />
            <WeightChart state={state} />
          </>
        )}
        {tab === "log" && (
          <>
            <WeightQuickInput onSave={addWeightEntry} todayEntry={todayWeight} />
            <DailyChecklist log={todayLog} onChange={(patch) => setDailyLog(today, patch)} />
            <SideEffectLog state={state} onAdd={addSideEffect} onRemove={removeSideEffect} />
          </>
        )}
        {tab === "guide" && <QuickReference />}
      </main>
      <TabNav active={tab} onChange={setTab} />
    </div>
  );
}
