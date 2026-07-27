"use client";

import { useState } from "react";
import { todayISO, weekdayName, weekdayOf } from "@/lib/date";
import { Card } from "./ui";

export function SetupForm({
  onComplete,
}: {
  onComplete: (startWeight: number, goalWeight: number, startDate: string, injectionDay: number) => void;
}) {
  const [startWeight, setStartWeight] = useState("117");
  const [goalWeight, setGoalWeight] = useState("107");
  const [startDate, setStartDate] = useState(todayISO());
  const [injectionDay, setInjectionDay] = useState(weekdayOf(todayISO()));

  return (
    <div className="mx-auto flex min-h-screen max-w-md flex-col justify-center gap-6 p-6">
      <div>
        <h1 className="text-2xl font-bold text-white">Velkommen til dit Wegovy-forløb</h1>
        <p className="mt-1 text-sm text-slate-400">Udfyld dine startdata for at komme i gang. Alt gemmes kun lokalt på din enhed.</p>
      </div>
      <Card className="flex flex-col gap-4">
        <label className="flex flex-col gap-1">
          <span className="text-sm text-slate-300">Startvægt (kg)</span>
          <input
            type="number"
            inputMode="decimal"
            value={startWeight}
            onChange={(e) => setStartWeight(e.target.value)}
            className="rounded-lg border border-surface-border bg-surface px-3 py-2 text-white"
          />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-sm text-slate-300">Målvægt (kg)</span>
          <input
            type="number"
            inputMode="decimal"
            value={goalWeight}
            onChange={(e) => setGoalWeight(e.target.value)}
            className="rounded-lg border border-surface-border bg-surface px-3 py-2 text-white"
          />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-sm text-slate-300">Startdato</span>
          <input
            type="date"
            value={startDate}
            onChange={(e) => {
              setStartDate(e.target.value);
              setInjectionDay(weekdayOf(e.target.value));
            }}
            className="rounded-lg border border-surface-border bg-surface px-3 py-2 text-white"
          />
        </label>
        <label className="flex flex-col gap-1">
          <span className="text-sm text-slate-300">Fast injektionsdag</span>
          <select
            value={injectionDay}
            onChange={(e) => setInjectionDay(Number(e.target.value))}
            className="rounded-lg border border-surface-border bg-surface px-3 py-2 text-white"
          >
            {[0, 1, 2, 3, 4, 5, 6].map((d) => (
              <option key={d} value={d}>
                {weekdayName(d)}
              </option>
            ))}
          </select>
        </label>
        <button
          onClick={() => onComplete(Number(startWeight), Number(goalWeight), startDate, injectionDay)}
          disabled={!startWeight || !goalWeight || !startDate}
          className="mt-2 rounded-lg bg-emerald-500 px-4 py-3 font-semibold text-black disabled:opacity-40"
        >
          Start forløb
        </button>
      </Card>
    </div>
  );
}
