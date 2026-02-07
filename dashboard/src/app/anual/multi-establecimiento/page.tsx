"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Users, AlertTriangle } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { PageTransition } from "@/components/motion/page-transition";
import {
  getAnualYears,
  getAnualMultiEstablishment,
  AnualMultiEstEntry,
} from "@/lib/api";
import { formatCLP } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

const MONTH_LABELS: Record<string, string> = {
  "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
  "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
  "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
};

const COLORS = [
  "bg-blue-500", "bg-emerald-500", "bg-amber-500", "bg-purple-500",
  "bg-rose-500", "bg-cyan-500", "bg-orange-500", "bg-teal-500",
];

function getMonthLabel(mes: string): string {
  const parts = mes.split("-");
  if (parts.length === 2) {
    return MONTH_LABELS[parts[1]] || parts[1];
  }
  return mes;
}

export default function MultiEstablecimientoAnualPage() {
  return (
    <Suspense fallback={<div className="p-8 text-center text-muted-foreground">Cargando...</div>}>
      <MultiEstablecimientoContent />
    </Suspense>
  );
}

function MultiEstablecimientoContent() {
  const searchParams = useSearchParams();
  const anioParam = searchParams.get("anio");

  const [years, setYears] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number>(
    anioParam ? parseInt(anioParam) : new Date().getFullYear()
  );
  const [docentes, setDocentes] = useState<AnualMultiEstEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    getAnualYears().then((y) => { if (!cancelled) setYears(y); }).catch(() => {});
    return () => { cancelled = true; };
  }, []);

  useEffect(() => {
    if (!selectedYear) return;
    let cancelled = false;
    setLoading(true);
    getAnualMultiEstablishment(selectedYear)
      .then((d) => { if (!cancelled) setDocentes(d); })
      .catch(() => { if (!cancelled) setDocentes([]); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [selectedYear]);

  // Collect all months for timeline
  const allMonths = Array.from({ length: 12 }, (_, i) =>
    `${selectedYear}-${String(i + 1).padStart(2, "0")}`
  );

  return (
    <PageTransition>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Multi-Establecimiento Anual</h1>
            <p className="text-muted-foreground text-sm">
              Docentes que trabajan en 2 o mas establecimientos durante el ano
            </p>
          </div>
          <Select
            value={selectedYear.toString()}
            onValueChange={(v) => setSelectedYear(parseInt(v))}
          >
            <SelectTrigger className="w-32">
              <SelectValue placeholder="Ano" />
            </SelectTrigger>
            <SelectContent>
              {years.map((y) => (
                <SelectItem key={y} value={y.toString()}>
                  {y}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {loading ? (
          <p className="text-center text-muted-foreground py-8">Cargando...</p>
        ) : docentes.length === 0 ? (
          <Card>
            <CardContent className="py-12 text-center">
              <Users className="h-10 w-10 mx-auto text-muted-foreground/50 mb-3" />
              <p className="text-lg font-medium mb-1">Sin docentes multi-establecimiento</p>
              <p className="text-sm text-muted-foreground">
                No se encontraron docentes en 2+ establecimientos para {selectedYear}
              </p>
            </CardContent>
          </Card>
        ) : (
          <>
            <Card>
              <CardContent className="pt-4 pb-3">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="h-5 w-5 text-amber-500" />
                  <span className="font-medium">{docentes.length}</span>
                  <span className="text-muted-foreground">
                    docente(s) en multiples establecimientos durante {selectedYear}
                  </span>
                </div>
              </CardContent>
            </Card>

            {docentes.map((d) => {
              // Create color map for establishments
              const colorMap: Record<string, string> = {};
              d.establecimientos.forEach((est, i) => {
                colorMap[est.rbd] = COLORS[i % COLORS.length];
              });

              return (
                <Card key={d.rut}>
                  <CardHeader className="pb-2">
                    <div className="flex items-center justify-between">
                      <div>
                        <CardTitle className="text-base">{d.nombre}</CardTitle>
                        <p className="text-xs text-muted-foreground">{d.rut}</p>
                      </div>
                      <div className="text-right">
                        <p className="font-bold">{formatCLP(d.total_brp)}</p>
                        <p className="text-xs text-muted-foreground">BRP Total</p>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    {/* Establishments */}
                    <div className="space-y-2">
                      {d.establecimientos.map((est) => (
                        <div
                          key={est.rbd}
                          className="flex items-center justify-between text-sm"
                        >
                          <div className="flex items-center gap-2">
                            <div className={`h-3 w-3 rounded-full ${colorMap[est.rbd]}`} />
                            <span>{est.escuela}</span>
                            <Badge variant="outline" className="text-[10px]">
                              {est.rbd}
                            </Badge>
                          </div>
                          <span className="font-medium">{formatCLP(est.brp_total)}</span>
                        </div>
                      ))}
                    </div>

                    {/* Timeline */}
                    <div>
                      <p className="text-xs text-muted-foreground mb-2">Timeline mensual</p>
                      <div className="flex gap-0.5">
                        {allMonths.map((month) => {
                          // Find which establishment(s) this teacher was in this month
                          const matching = d.establecimientos.filter((est) =>
                            est.meses.includes(month)
                          );
                          const color =
                            matching.length > 1
                              ? "bg-red-500" // Multiple schools same month
                              : matching.length === 1
                              ? colorMap[matching[0].rbd]
                              : "bg-muted";

                          return (
                            <div
                              key={month}
                              className="flex-1 flex flex-col items-center gap-1"
                              title={
                                matching.length > 0
                                  ? matching.map((e) => e.escuela).join(" + ")
                                  : "Sin actividad"
                              }
                            >
                              <div
                                className={`w-full h-6 rounded-sm ${color} ${
                                  matching.length === 0 ? "opacity-30" : ""
                                }`}
                              />
                              <span className="text-[9px] text-muted-foreground">
                                {getMonthLabel(month)}
                              </span>
                            </div>
                          );
                        })}
                      </div>
                      <div className="flex gap-3 mt-2 text-[10px] text-muted-foreground">
                        {d.establecimientos.map((est) => (
                          <div key={est.rbd} className="flex items-center gap-1">
                            <div className={`h-2 w-2 rounded-full ${colorMap[est.rbd]}`} />
                            <span>{est.escuela}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </>
        )}
      </div>
    </PageTransition>
  );
}
