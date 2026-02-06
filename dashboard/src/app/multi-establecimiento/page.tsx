"use client";

import { Building2, BarChart3 } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { MultiEstablishmentTable } from "@/components/multi-establishment-table";
import { StatsCard } from "@/components/stats-card";
import { PageTransition } from "@/components/motion/page-transition";
import { EmptyState } from "@/components/motion/empty-state";
import { StaggerContainer, StaggerItem } from "@/components/motion/stagger-children";
import { useAppState } from "@/lib/store";
import { formatCLP } from "@/lib/utils";

export default function MultiEstablecimientoPage() {
  const { multiEstablishment, sessionId, loadDemoData } = useAppState();

  if (!sessionId) {
    return (
      <PageTransition>
        <EmptyState
          icon={Building2}
          title="No hay datos disponibles"
          description="Suba y procese archivos para ver docentes multi-establecimiento."
          onDemo={loadDemoData}
        />
      </PageTransition>
    );
  }

  const totalMultiBRP = multiEstablishment.reduce((sum, r) => sum + r.total_brp, 0);
  const totalMultiHoras = multiEstablishment.reduce((sum, r) => sum + r.total_horas, 0);
  const exceededCount = multiEstablishment.filter((r) => r.total_horas > 44).length;

  // Calculate per-subsidy breakdown across all multi-establishment teachers
  const subsidyTotals = multiEstablishment.reduce(
    (acc, teacher) => {
      for (const est of teacher.establecimientos) {
        // If establecimientos have brp_sep/pie/normal fields, accumulate them
        const e = est as Record<string, unknown>;
        acc.brp_sep += Number(e.brp_sep ?? 0);
        acc.brp_pie += Number(e.brp_pie ?? 0);
        acc.brp_normal += Number(e.brp_normal ?? 0);
      }
      return acc;
    },
    { brp_sep: 0, brp_pie: 0, brp_normal: 0 }
  );

  return (
    <PageTransition>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Multi-Establecimiento</h1>
          <p className="text-muted-foreground text-sm">
            Docentes que trabajan en 2 o mas establecimientos educacionales
          </p>
        </div>

        {multiEstablishment.length > 0 && (
          <>
            <StaggerContainer className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <StaggerItem>
                <StatsCard
                  title="Docentes Multi-Establecimiento"
                  value={multiEstablishment.length.toString()}
                  icon={Building2}
                  color="text-primary"
                />
              </StaggerItem>
              <StaggerItem>
                <StatsCard
                  title="BRP Total Multi-Estab."
                  value={formatCLP(totalMultiBRP)}
                  numericValue={totalMultiBRP}
                  formatFn={(n) => formatCLP(n)}
                  icon={BarChart3}
                  color="text-emerald-500"
                />
              </StaggerItem>
              <StaggerItem>
                <StatsCard
                  title="Exceden 44 Horas"
                  value={exceededCount.toString()}
                  subtitle={exceededCount > 0 ? "Requieren revision" : "Ninguno"}
                  icon={Building2}
                  color={exceededCount > 0 ? "text-amber-500" : "text-emerald-500"}
                />
              </StaggerItem>
            </StaggerContainer>

            {/* Subsidy breakdown for multi-establishment */}
            {(subsidyTotals.brp_sep > 0 || subsidyTotals.brp_pie > 0 || subsidyTotals.brp_normal > 0) && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Desglose por Subvencion</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-3 gap-4">
                    <div className="text-center p-3 rounded-lg bg-blue-500/5">
                      <Badge variant="sep" className="mb-2">SEP</Badge>
                      <p className="text-lg font-bold text-blue-600 dark:text-blue-400">
                        {formatCLP(subsidyTotals.brp_sep)}
                      </p>
                    </div>
                    <div className="text-center p-3 rounded-lg bg-emerald-500/5">
                      <Badge variant="pie" className="mb-2">PIE</Badge>
                      <p className="text-lg font-bold text-emerald-600 dark:text-emerald-400">
                        {formatCLP(subsidyTotals.brp_pie)}
                      </p>
                    </div>
                    <div className="text-center p-3 rounded-lg bg-amber-500/5">
                      <Badge variant="normal" className="mb-2">Normal</Badge>
                      <p className="text-lg font-bold text-amber-600 dark:text-amber-400">
                        {formatCLP(subsidyTotals.brp_normal)}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}
          </>
        )}

        <MultiEstablishmentTable data={multiEstablishment} />
      </div>
    </PageTransition>
  );
}
