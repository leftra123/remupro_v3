"use client";

import { ClipboardList, Bell, Settings } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { AuditLogTable } from "@/components/audit-log-table";
import { PageTransition } from "@/components/motion/page-transition";
import { EmptyState } from "@/components/motion/empty-state";
import { useAppState } from "@/lib/store";
import Link from "next/link";

export default function AuditoriaPage() {
  const { auditLog, sessionId, loadDemoData, columnPreferences } = useAppState();

  if (!sessionId) {
    return (
      <PageTransition>
        <EmptyState
          icon={ClipboardList}
          title="No hay registros de auditoria"
          description="Suba y procese archivos para ver el log de auditoria."
          onDemo={loadDemoData}
        />
      </PageTransition>
    );
  }

  // Separate column alerts from other audit entries
  const columnAlerts = auditLog.filter((e) => e.tipo === "COLUMNA_FALTANTE");
  const otherEntries = auditLog.filter((e) => e.tipo !== "COLUMNA_FALTANTE");

  // Check which column alerts are ignored
  const ignoredKeys = new Set(
    columnPreferences
      .filter((p) => p.estado === "ignore")
      .map((p) => p.columna_key)
  );

  const activeAlerts = columnAlerts.filter((a) => {
    // Try to extract column key from message
    const match = a.mensaje.match(/['"]([^'"]+)['"]/);
    return !match || !ignoredKeys.has(match[1]);
  });

  const ignoredAlerts = columnAlerts.filter((a) => {
    const match = a.mensaje.match(/['"]([^'"]+)['"]/);
    return match && ignoredKeys.has(match[1]);
  });

  return (
    <PageTransition>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Auditoria</h1>
            <p className="text-muted-foreground text-sm">
              Registro detallado de todas las operaciones y alertas del procesamiento BRP
            </p>
          </div>
          <Link href="/alertas">
            <Button variant="outline" size="sm">
              <Settings className="mr-2 h-4 w-4" />
              Configurar Alertas
            </Button>
          </Link>
        </div>

        {/* Column alerts section */}
        {columnAlerts.length > 0 && (
          <Card>
            <CardHeader className="pb-3">
              <div className="flex items-center gap-2">
                <Bell className="h-4 w-4 text-amber-500" />
                <CardTitle className="text-sm">Alertas de Columnas</CardTitle>
                <Badge variant="secondary" className="ml-auto">
                  {activeAlerts.length} activa{activeAlerts.length !== 1 ? "s" : ""}
                </Badge>
              </div>
            </CardHeader>
            <CardContent className="space-y-2">
              {activeAlerts.map((alert, i) => (
                <div
                  key={i}
                  className="flex items-center gap-3 p-2 rounded-lg border border-amber-500/20 bg-amber-500/5"
                >
                  <Badge
                    variant={alert.nivel === "ERROR" ? "destructive" : "outline"}
                    className="text-[10px] shrink-0"
                  >
                    {alert.nivel}
                  </Badge>
                  <span className="text-sm">{alert.mensaje}</span>
                </div>
              ))}
              {ignoredAlerts.length > 0 && (
                <details className="mt-2">
                  <summary className="text-xs text-muted-foreground cursor-pointer hover:text-foreground">
                    {ignoredAlerts.length} alerta{ignoredAlerts.length !== 1 ? "s" : ""} ignorada{ignoredAlerts.length !== 1 ? "s" : ""}
                  </summary>
                  <div className="mt-2 space-y-1 opacity-50">
                    {ignoredAlerts.map((alert, i) => (
                      <div key={i} className="flex items-center gap-3 p-2 rounded-lg border">
                        <Badge variant="secondary" className="text-[10px] shrink-0">
                          Ignorada
                        </Badge>
                        <span className="text-xs line-through">{alert.mensaje}</span>
                      </div>
                    ))}
                  </div>
                </details>
              )}
            </CardContent>
          </Card>
        )}

        <AuditLogTable data={otherEntries.length > 0 ? otherEntries : auditLog} />
      </div>
    </PageTransition>
  );
}
