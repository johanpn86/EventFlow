"use client";

import { useMemo } from "react";
import { addDays, daysBetween, todayISO } from "@/lib/date";
import type { AppState } from "@/lib/types";
import { Card, SectionTitle, StatBlock } from "./ui";

const PROGRAM_WEEKS = 16;
const WIDTH = 320;
const HEIGHT = 160;
const PAD = 24;

export function WeightChart({ state }: { state: AppState }) {
  const latest = state.weightLogs[state.weightLogs.length - 1];
  const currentWeight = latest?.weightKg ?? state.startWeight;
  const totalLossKg = state.startWeight - currentWeight;
  const totalLossPct = state.startWeight > 0 ? (totalLossKg / state.startWeight) * 100 : 0;
  const goalPct = state.startWeight > 0 ? ((state.startWeight - state.goalWeight) / state.startWeight) * 100 : 0;

  const { targetPoints, actualPoints, minY, maxY } = useMemo(() => {
    const endDate = addDays(state.startDate, PROGRAM_WEEKS * 7);
    const totalDays = daysBetween(state.startDate, endDate);
    const weeklyLossKg = (state.startWeight - state.goalWeight) / PROGRAM_WEEKS;

    const target: { x: number; y: number }[] = [];
    for (let w = 0; w <= PROGRAM_WEEKS; w++) {
      target.push({ x: (w * 7) / totalDays, y: state.startWeight - weeklyLossKg * w });
    }

    const actual = state.weightLogs.map((entry) => ({
      x: Math.max(0, daysBetween(state.startDate, entry.date)) / totalDays,
      y: entry.weightKg,
    }));

    const allY = [...target.map((p) => p.y), ...actual.map((p) => p.y)];
    return {
      targetPoints: target,
      actualPoints: actual,
      minY: Math.min(...allY) - 1,
      maxY: Math.max(...allY) + 1,
    };
  }, [state.startDate, state.startWeight, state.goalWeight, state.weightLogs]);

  function toSvg(p: { x: number; y: number }) {
    const x = PAD + p.x * (WIDTH - PAD * 2);
    const y = HEIGHT - PAD - ((p.y - minY) / (maxY - minY)) * (HEIGHT - PAD * 2);
    return `${x},${y}`;
  }

  const targetPath = targetPoints.map(toSvg).join(" ");
  const actualPath = actualPoints.map(toSvg).join(" ");
  const todayX = PAD + Math.min(1, daysBetween(state.startDate, todayISO()) / (PROGRAM_WEEKS * 7)) * (WIDTH - PAD * 2);

  return (
    <Card>
      <SectionTitle>Vægt vs. mållinje</SectionTitle>
      <div className="grid grid-cols-3 gap-3">
        <StatBlock label="Nu" value={`${currentWeight.toFixed(1)} kg`} />
        <StatBlock label="Tabt" value={`${totalLossKg.toFixed(1)} kg`} sub={`${totalLossPct.toFixed(1)}%`} />
        <StatBlock label="Mål" value={`${goalPct.toFixed(1)}%`} sub={`af kropsvægt`} />
      </div>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="mt-4 w-full" role="img" aria-label="Vægtgraf">
        <line x1={todayX} y1={PAD} x2={todayX} y2={HEIGHT - PAD} stroke="#232c37" strokeDasharray="3 3" />
        <polyline points={targetPath} fill="none" stroke="#475569" strokeWidth={2} strokeDasharray="5 4" />
        {actualPoints.length > 0 && (
          <polyline points={actualPath} fill="none" stroke="#10b981" strokeWidth={2.5} />
        )}
        {actualPoints.map((p, i) => (
          <circle key={i} cx={toSvg(p).split(",")[0]} cy={toSvg(p).split(",")[1]} r={3} fill="#10b981" />
        ))}
      </svg>
      <div className="flex items-center gap-4 text-xs text-slate-400">
        <span className="flex items-center gap-1">
          <span className="h-0.5 w-4 bg-emerald-500" /> Faktisk
        </span>
        <span className="flex items-center gap-1">
          <span className="h-0.5 w-4 border-t-2 border-dashed border-slate-500" /> Mållinje (-0,625 kg/uge)
        </span>
      </div>
    </Card>
  );
}
