"use client";

export type TabKey = "dashboard" | "log" | "guide";

const TABS: { key: TabKey; label: string; icon: string }[] = [
  { key: "dashboard", label: "Dashboard", icon: "📊" },
  { key: "log", label: "Log", icon: "📝" },
  { key: "guide", label: "Guide", icon: "📋" },
];

export function TabNav({ active, onChange }: { active: TabKey; onChange: (tab: TabKey) => void }) {
  return (
    <nav className="fixed inset-x-0 bottom-0 z-10 border-t border-surface-border bg-surface-raised/95 backdrop-blur">
      <div className="mx-auto flex max-w-md">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => onChange(tab.key)}
            className={`flex flex-1 flex-col items-center gap-0.5 py-3 text-xs font-medium ${
              active === tab.key ? "text-emerald-400" : "text-slate-500"
            }`}
          >
            <span className="text-lg leading-none">{tab.icon}</span>
            {tab.label}
          </button>
        ))}
      </div>
    </nav>
  );
}
