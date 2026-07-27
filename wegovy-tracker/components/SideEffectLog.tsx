"use client";

import { useState } from "react";
import { formatDateDa, todayISO } from "@/lib/date";
import { calendarWeek, phaseForWeek } from "@/lib/dosePlan";
import { SIDE_EFFECT_TAGS, type AppState, type SideEffectEntry, type SideEffectTag } from "@/lib/types";
import { Card, SectionTitle } from "./ui";

function makeId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function SideEffectLog({
  state,
  onAdd,
  onRemove,
}: {
  state: AppState;
  onAdd: (entry: SideEffectEntry) => void;
  onRemove: (id: string) => void;
}) {
  const [selectedTag, setSelectedTag] = useState<SideEffectTag | null>(null);
  const [severity, setSeverity] = useState(3);

  function submit() {
    if (!selectedTag) return;
    onAdd({ id: makeId(), date: todayISO(), tag: selectedTag, severity: severity as 1 | 2 | 3 | 4 | 5 });
    setSelectedTag(null);
    setSeverity(3);
  }

  return (
    <Card className="flex flex-col gap-4">
      <SectionTitle>Bivirknings-log</SectionTitle>

      <div className="flex flex-wrap gap-2">
        {SIDE_EFFECT_TAGS.map((tag) => (
          <button
            key={tag}
            onClick={() => setSelectedTag(tag)}
            className={`rounded-full px-3 py-1.5 text-sm ${
              selectedTag === tag ? "bg-emerald-500 text-black" : "border border-surface-border text-slate-300"
            }`}
          >
            {tag}
          </button>
        ))}
      </div>

      {selectedTag && (
        <div className="rounded-xl border border-surface-border p-3">
          <div className="mb-2 flex items-center justify-between text-sm">
            <span className="text-slate-300">Sværhedsgrad</span>
            <span className="font-semibold text-white">{severity} / 5</span>
          </div>
          <input
            type="range"
            min={1}
            max={5}
            value={severity}
            onChange={(e) => setSeverity(Number(e.target.value))}
            className="w-full"
          />
          <button onClick={submit} className="mt-3 w-full rounded-lg bg-emerald-500 px-3 py-2 text-sm font-semibold text-black">
            Log {selectedTag}
          </button>
        </div>
      )}

      <div className="flex flex-col gap-2">
        {state.sideEffectLogs.length === 0 && <p className="text-sm text-slate-500">Ingen bivirkninger logget endnu.</p>}
        {state.sideEffectLogs.slice(0, 20).map((entry) => {
          const phase = phaseForWeek(calendarWeek(state, entry.date));
          return (
            <div key={entry.id} className="flex items-center justify-between rounded-xl border border-surface-border p-2.5">
              <div className="flex items-center gap-2">
                <span className={`h-2.5 w-2.5 rounded-full ${phase.penColorClass}`} />
                <div>
                  <p className="text-sm font-medium text-white">{entry.tag}</p>
                  <p className="text-xs text-slate-500">
                    {formatDateDa(entry.date)} · sværhed {entry.severity}/5 · {phase.doseLabel}
                  </p>
                </div>
              </div>
              <button onClick={() => onRemove(entry.id)} className="text-xs text-slate-500 hover:text-rose-400">
                Slet
              </button>
            </div>
          );
        })}
      </div>
    </Card>
  );
}
