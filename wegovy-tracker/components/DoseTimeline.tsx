"use client";

import { DOSE_PHASES, calendarWeek } from "@/lib/dosePlan";
import type { AppState } from "@/lib/types";
import { Card, SectionTitle } from "./ui";

export function DoseTimeline({
  state,
  onLock,
  onUnlock,
}: {
  state: AppState;
  onLock: () => void;
  onUnlock: () => void;
}) {
  const week = calendarWeek(state);
  const locked = Boolean(state.lockStartedAt);

  return (
    <Card>
      <SectionTitle>Dosis- og optrapningsplan</SectionTitle>
      <div className="flex flex-col gap-2">
        {DOSE_PHASES.map((phase) => {
          const isCurrent = week >= phase.weekStart && (phase.weekEnd === null || week <= phase.weekEnd);
          const weekRange = phase.weekEnd ? `Uge ${phase.weekStart}-${phase.weekEnd}` : `Uge ${phase.weekStart}+`;
          return (
            <div
              key={phase.index}
              className={`flex items-center gap-3 rounded-xl border p-3 transition-colors ${
                isCurrent ? "border-emerald-500 bg-emerald-500/10" : "border-surface-border"
              }`}
            >
              <span className={`h-3 w-3 shrink-0 rounded-full ${phase.penColorClass}`} />
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <span className="font-semibold text-white">{phase.doseLabel}</span>
                  <span className="text-xs text-slate-400">{phase.penLabel}</span>
                </div>
                <span className="text-xs text-slate-500">{weekRange}</span>
              </div>
              {isCurrent && (
                <span className="rounded-full bg-emerald-500 px-2 py-0.5 text-xs font-semibold text-black">
                  Nu · uge {week}
                </span>
              )}
            </div>
          );
        })}
      </div>
      <div className="mt-4 flex items-center justify-between rounded-xl border border-surface-border p-3">
        <div>
          <p className="text-sm font-medium text-white">{locked ? "Optrapning er låst" : "Lås nuværende dosis"}</p>
          <p className="text-xs text-slate-500">
            {locked
              ? "Ugen tæller ikke videre før du genoptager (aftalt med læge ved bivirkninger)."
              : "Udskyd optrapning hvis du og din læge vurderer det nødvendigt."}
          </p>
        </div>
        <button
          onClick={locked ? onUnlock : onLock}
          className={`shrink-0 rounded-lg px-3 py-2 text-sm font-semibold ${
            locked ? "bg-emerald-500 text-black" : "border border-surface-border text-slate-200"
          }`}
        >
          {locked ? "Genoptag" : "Lås dosis"}
        </button>
      </div>
    </Card>
  );
}
