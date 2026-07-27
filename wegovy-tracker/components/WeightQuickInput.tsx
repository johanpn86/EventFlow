"use client";

import { useState } from "react";
import { todayISO } from "@/lib/date";
import type { WeightEntry } from "@/lib/types";
import { Card, SectionTitle } from "./ui";

export function WeightQuickInput({ onSave, todayEntry }: { onSave: (entry: WeightEntry) => void; todayEntry?: WeightEntry }) {
  const [value, setValue] = useState(todayEntry ? String(todayEntry.weightKg) : "");

  function save() {
    const weightKg = Number(value.replace(",", "."));
    if (!weightKg || weightKg <= 0) return;
    onSave({ date: todayISO(), weightKg });
  }

  return (
    <Card>
      <SectionTitle>Dagens vægt</SectionTitle>
      <div className="flex gap-2">
        <input
          type="number"
          inputMode="decimal"
          step="0.1"
          placeholder="kg"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          className="w-full rounded-lg border border-surface-border bg-surface px-3 py-3 text-lg text-white"
        />
        <button onClick={save} className="shrink-0 rounded-lg bg-emerald-500 px-5 font-semibold text-black">
          Gem
        </button>
      </div>
    </Card>
  );
}
