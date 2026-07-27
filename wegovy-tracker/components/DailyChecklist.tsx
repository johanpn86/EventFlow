"use client";

import type { DailyLog } from "@/lib/types";
import { Card, ProgressBar, SectionTitle } from "./ui";

const WATER_GOAL_ML = 3000;
const WATER_STEP_ML = 250;

export function DailyChecklist({
  log,
  onChange,
}: {
  log: DailyLog;
  onChange: (patch: Partial<DailyLog>) => void;
}) {
  const waterPct = (log.waterMl / WATER_GOAL_ML) * 100;

  return (
    <Card className="flex flex-col gap-5">
      <SectionTitle>Daglig tjekliste</SectionTitle>

      <div>
        <div className="mb-1 flex items-center justify-between">
          <span className="text-sm font-medium text-white">Væske</span>
          <span className="text-sm text-slate-400">
            {(log.waterMl / 1000).toFixed(2)} / {(WATER_GOAL_ML / 1000).toFixed(1)} L
          </span>
        </div>
        <ProgressBar value={waterPct} />
        <div className="mt-2 flex gap-2">
          <button
            onClick={() => onChange({ waterMl: Math.max(0, log.waterMl - WATER_STEP_ML) })}
            className="rounded-lg border border-surface-border px-3 py-1.5 text-lg text-slate-200"
          >
            −
          </button>
          <button
            onClick={() => onChange({ waterMl: log.waterMl + WATER_STEP_ML })}
            className="flex-1 rounded-lg bg-emerald-500/90 px-3 py-1.5 text-sm font-semibold text-black"
          >
            + {WATER_STEP_ML} ml
          </button>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-white">Protein-mål (120g+)</span>
        <div className="flex gap-2">
          <button
            onClick={() => onChange({ proteinHit: true })}
            className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
              log.proteinHit === true ? "bg-emerald-500 text-black" : "border border-surface-border text-slate-300"
            }`}
          >
            Ja
          </button>
          <button
            onClick={() => onChange({ proteinHit: false })}
            className={`rounded-lg px-3 py-1.5 text-sm font-semibold ${
              log.proteinHit === false ? "bg-rose-500 text-black" : "border border-surface-border text-slate-300"
            }`}
          >
            Nej
          </button>
        </div>
      </div>

      <div className="flex items-center justify-between">
        <span className="text-sm font-medium text-white">Styrketræning gennemført</span>
        <button
          onClick={() => onChange({ strength: !log.strength })}
          className={`h-7 w-12 rounded-full transition-colors ${log.strength ? "bg-emerald-500" : "bg-surface-border"}`}
        >
          <span
            className={`block h-6 w-6 translate-x-0.5 rounded-full bg-white transition-transform ${
              log.strength ? "translate-x-5" : ""
            }`}
          />
        </button>
      </div>
    </Card>
  );
}
