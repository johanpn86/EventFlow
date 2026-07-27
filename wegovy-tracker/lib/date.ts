const DAY_MS = 24 * 60 * 60 * 1000;

export function todayISO(): string {
  return toISODate(new Date());
}

export function toISODate(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

export function parseISODate(iso: string): Date {
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(y, m - 1, d);
}

export function daysBetween(fromISO: string, toISOStr: string): number {
  const a = parseISODate(fromISO).getTime();
  const b = parseISODate(toISOStr).getTime();
  return Math.round((b - a) / DAY_MS);
}

export function addDays(iso: string, days: number): string {
  const d = parseISODate(iso);
  d.setDate(d.getDate() + days);
  return toISODate(d);
}

const WEEKDAYS_DA = ["Søndag", "Mandag", "Tirsdag", "Onsdag", "Torsdag", "Fredag", "Lørdag"];

export function weekdayName(day: number): string {
  return WEEKDAYS_DA[day];
}

export function weekdayOf(iso: string): number {
  return parseISODate(iso).getDay();
}

export function nextOccurrenceOfWeekday(fromISO: string, weekday: number): string {
  const current = weekdayOf(fromISO);
  let offset = (weekday - current + 7) % 7;
  return addDays(fromISO, offset);
}

export function formatDateDa(iso: string): string {
  const d = parseISODate(iso);
  return d.toLocaleDateString("da-DK", { day: "numeric", month: "short", year: "numeric" });
}

export function hoursBetween(fromISO: string, toISOStr: string): number {
  return daysBetween(fromISO, toISOStr) * 24;
}
