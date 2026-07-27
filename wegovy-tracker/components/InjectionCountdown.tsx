"use client";

import { useMemo, useState } from "react";
import { formatDateDa, hoursBetween, nextOccurrenceOfWeekday, todayISO, weekdayName } from "@/lib/date";
import type { AppState } from "@/lib/types";
import { Card, SectionTitle } from "./ui";

export function InjectionCountdown({
  state,
  onChangeDay,
  onLogInjection,
}: {
  state: AppState;
  onChangeDay: (day: number) => void;
  onLogInjection: (date: string) => void;
}) {
  const today = todayISO();
  const nextDate = useMemo(() => nextOccurrenceOfWeekday(today, state.injectionDay), [today, state.injectionDay]);
  const daysLeft = Math.round((new Date(nextDate).getTime() - new Date(today).getTime()) / 86400000);
  const isToday = nextDate === today;
  const lastInjection = state.injectionLog[state.injectionLog.length - 1];

  const [pendingDay, setPendingDay] = useState<number | null>(null);

  const pendingWarningHours = useMemo(() => {
    if (pendingDay === null || !lastInjection) return null;
    const candidate = nextOccurrenceOfWeekday(today, pendingDay);
    const hours = hoursBetween(lastInjection, candidate);
    return hours < 72 ? hours : null;
  }, [pendingDay, lastInjection, today]);

  function handleSelect(day: number) {
    if (day === state.injectionDay) return;
    setPendingDay(day);
  }

  function confirmChange() {
    if (pendingDay === null) return;
    onChangeDay(pendingDay);
    setPendingDay(null);
  }

  return (
    <Card>
      <SectionTitle>Næste stikdag</SectionTitle>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-3xl font-bold text-white">
            {isToday ? "I dag" : `${daysLeft} ${daysLeft === 1 ? "dag" : "dage"}`}
          </p>
          <p className="text-sm text-slate-400">{formatDateDa(nextDate)} · {weekdayName(state.injectionDay)}</p>
        </div>
        {isToday && (
          <button
            onClick={() => onLogInjection(today)}
            disabled={state.injectionLog.includes(today)}
            className="rounded-lg bg-emerald-500 px-3 py-2 text-sm font-semibold text-black disabled:opacity-40"
          >
            {state.injectionLog.includes(today) ? "Logget ✓" : "Marker som taget"}
          </button>
        )}
      </div>

      <div className="mt-4">
        <label className="mb-1 block text-sm text-slate-300">Skift fast injektionsdag</label>
        <select
          value={pendingDay ?? state.injectionDay}
          onChange={(e) => handleSelect(Number(e.target.value))}
          className="w-full rounded-lg border border-surface-border bg-surface px-3 py-2 text-white"
        >
          {[0, 1, 2, 3, 4, 5, 6].map((d) => (
            <option key={d} value={d}>
              {weekdayName(d)}
            </option>
          ))}
        </select>
      </div>

      {pendingDay !== null && (
        <div className="mt-3 rounded-xl border border-amber-500/50 bg-amber-500/10 p-3 text-sm">
          {pendingWarningHours !== null ? (
            <>
              <p className="font-semibold text-amber-400">
                Advarsel: kun {Math.max(0, Math.round(pendingWarningHours))} timer siden sidste stik.
              </p>
              <p className="mt-1 text-amber-200/80">
                Der anbefales minimum 72 timers afstand mellem stik. Vil du alligevel skifte dag?
              </p>
            </>
          ) : (
            <p className="text-slate-300">Bekræft skift af injektionsdag til {weekdayName(pendingDay)}.</p>
          )}
          <div className="mt-2 flex gap-2">
            <button onClick={confirmChange} className="rounded-lg bg-amber-500 px-3 py-1.5 text-sm font-semibold text-black">
              Bekræft skift
            </button>
            <button onClick={() => setPendingDay(null)} className="rounded-lg border border-surface-border px-3 py-1.5 text-sm text-slate-300">
              Fortryd
            </button>
          </div>
        </div>
      )}
    </Card>
  );
}
