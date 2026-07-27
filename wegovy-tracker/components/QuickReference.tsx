"use client";

import { useState } from "react";
import { Card, SectionTitle } from "./ui";

const INJECTION_STEPS = [
  "Væske stuetempereret?",
  "Flowtjek udført (ved ny pen)?",
  "Stik vinkelret (90°), tryk i bund, tæl til 6 før udtræk.",
  "Skiftet stiksted fra sidste uge?",
];

const RED_FLAGS = [
  "Fed/friteret mad (udløser akut kvalme/opkast).",
  "Sukkerbomber & sodavand m. brus (oppustethed).",
  "Rygning/nikotin på tom mave (udløser mavesyre/kvalme).",
  "At lægge sig ned inden for 2-3 timer efter måltid.",
];

export function QuickReference() {
  const [checked, setChecked] = useState<boolean[]>(INJECTION_STEPS.map(() => false));

  function toggle(i: number) {
    setChecked((prev) => prev.map((v, idx) => (idx === i ? !v : v)));
  }

  return (
    <div className="flex flex-col gap-4">
      <Card>
        <SectionTitle>Injektions-tjekliste</SectionTitle>
        <div className="flex flex-col gap-2">
          {INJECTION_STEPS.map((step, i) => (
            <label key={i} className="flex items-start gap-3 rounded-xl border border-surface-border p-3">
              <input
                type="checkbox"
                checked={checked[i]}
                onChange={() => toggle(i)}
                className="mt-0.5 h-5 w-5 accent-emerald-500"
              />
              <span className={`text-sm ${checked[i] ? "text-slate-500 line-through" : "text-slate-200"}`}>{step}</span>
            </label>
          ))}
        </div>
      </Card>

      <Card>
        <SectionTitle>Røde flag / trigger-mad (undgå)</SectionTitle>
        <ul className="flex flex-col gap-2">
          {RED_FLAGS.map((flag) => (
            <li key={flag} className="flex gap-2 text-sm text-slate-200">
              <span className="text-rose-400">•</span>
              {flag}
            </li>
          ))}
        </ul>
        <div className="mt-3 rounded-xl bg-amber-500/10 p-3 text-sm font-medium text-amber-300">
          Huskeregel: Stop ved 80% mæthed.
        </div>
      </Card>

      <Card>
        <SectionTitle>Substans- & sikkerhedsoverblik</SectionTitle>
        <div className="flex flex-col gap-3 text-sm text-slate-200">
          <div>
            <p className="font-semibold text-white">Rekreative stoffer / edibles / piller</p>
            <p className="text-slate-400">Forsinket optagelse i mavesækken — risiko for &ldquo;late hit&rdquo;.</p>
          </div>
          <div>
            <p className="font-semibold text-white">Centralstimulerende / alkohol</p>
            <p className="text-slate-400">
              Øget risiko for dehydrering og overophedning. Dæmpet dopamin-respons — pas på overdosering ved at søge
              sædvanlig rus.
            </p>
          </div>
        </div>
      </Card>
    </div>
  );
}
