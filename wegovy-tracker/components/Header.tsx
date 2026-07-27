"use client";

import { calendarWeek, currentPhase } from "@/lib/dosePlan";
import type { AppState } from "@/lib/types";

export function Header({ state }: { state: AppState }) {
  const week = calendarWeek(state);
  const phase = currentPhase(state);
  const weekOfProgram = Math.min(16, Math.max(1, week));

  return (
    <header className="sticky top-0 z-10 border-b border-surface-border bg-surface/90 px-4 py-3 backdrop-blur">
      <div className="mx-auto flex max-w-md items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-wide text-slate-500">Uge {weekOfProgram} af 16</p>
          <p className="text-lg font-bold text-white">{phase.doseLabel}</p>
        </div>
        <span className={`h-3 w-3 rounded-full ${phase.penColorClass}`} />
      </div>
    </header>
  );
}
