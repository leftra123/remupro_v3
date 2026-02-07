"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { PageTransition } from "@/components/motion/page-transition";
import { useAppState } from "@/lib/store";
import { getColumnPreferences, bulkUpdateColumnPreferences } from "@/lib/api";
import { Bell, Save, Loader2 } from "lucide-react";

const KNOWN_COLUMNS = [
  { key: "total_reconocimiento", name: "Total reconocimiento profesional", group: "critical" },
  { key: "total_tramo", name: "Total tramo", group: "critical" },
  { key: "subv_reconocimiento", name: "Subvencion reconocimiento (DAEM)", group: "critical" },
  { key: "transf_reconocimiento", name: "Transferencia reconocimiento (CPEIP)", group: "critical" },
  { key: "subv_tramo", name: "Subvencion tramo (DAEM)", group: "critical" },
  { key: "transf_tramo", name: "Transferencia tramo (CPEIP)", group: "critical" },
  { key: "asig_prioritarios", name: "Asignacion alumnos prioritarios (CPEIP)", group: "critical" },
  { key: "nombres", name: "Nombres del docente", group: "info" },
  { key: "apellido1", name: "Primer apellido", group: "info" },
  { key: "apellido2", name: "Segundo apellido", group: "info" },
  { key: "tipo_pago", name: "Tipo de pago", group: "info" },
  { key: "tramo", name: "Tramo", group: "info" },
];

type ColumnStatus = "default" | "ignore" | "important";

interface ColumnRow {
  key: string;
  name: string;
  group: string;
  status: ColumnStatus;
}

const STATUS_LABELS: Record<ColumnStatus, string> = {
  default: "Normal",
  ignore: "Ignorar",
  important: "Importante",
};

function StatusBadge({ status }: { status: ColumnStatus }) {
  switch (status) {
    case "ignore":
      return <Badge variant="secondary">{STATUS_LABELS.ignore}</Badge>;
    case "important":
      return <Badge variant="destructive">{STATUS_LABELS.important}</Badge>;
    default:
      return <Badge variant="outline">{STATUS_LABELS.default}</Badge>;
  }
}

export default function AlertasPage() {
  const { setColumnPreferences } = useAppState();
  const [columns, setColumns] = useState<ColumnRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);

  useEffect(() => {
    let cancelled = false;
    async function fetchPreferences() {
      setIsLoading(true);
      try {
        const prefs = await getColumnPreferences();
        if (cancelled) return;
        const prefMap = new Map(prefs.map((p) => [p.columna_key, p.estado as ColumnStatus]));

        const merged: ColumnRow[] = KNOWN_COLUMNS.map((col) => ({
          ...col,
          status: prefMap.get(col.key) || "default",
        }));

        // Add any preferences from the API that are not in KNOWN_COLUMNS
        const knownKeys = new Set(KNOWN_COLUMNS.map((c) => c.key));
        for (const pref of prefs) {
          if (!knownKeys.has(pref.columna_key)) {
            merged.push({
              key: pref.columna_key,
              name: pref.columna_key,
              group: "new",
              status: pref.estado as ColumnStatus,
            });
          }
        }

        setColumns(merged);
      } catch {
        if (cancelled) return;
        // If API fails, initialize with known columns at default status
        setColumns(
          KNOWN_COLUMNS.map((col) => ({
            ...col,
            status: "default" as ColumnStatus,
          }))
        );
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    }

    fetchPreferences();
    return () => { cancelled = true; };
  }, []);

  function handleStatusChange(key: string, newStatus: ColumnStatus) {
    setColumns((prev) =>
      prev.map((col) => (col.key === key ? { ...col, status: newStatus } : col))
    );
  }

  async function handleSave() {
    setIsSaving(true);
    try {
      const preferences = columns.map((col) => ({
        columna_key: col.key,
        estado: col.status,
      }));
      await bulkUpdateColumnPreferences(preferences);
      setColumnPreferences(
        columns.map((col) => ({
          columna_key: col.key,
          estado: col.status,
        }))
      );
    } catch {
      // Error handling - save failed silently
    } finally {
      setIsSaving(false);
    }
  }

  const criticalColumns = columns.filter((c) => c.group === "critical");
  const infoColumns = columns.filter((c) => c.group === "info");
  const newColumns = columns.filter((c) => c.group === "new");

  if (isLoading) {
    return (
      <PageTransition>
        <div className="flex items-center justify-center py-20">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          <span className="ml-3 text-muted-foreground">Cargando preferencias...</span>
        </div>
      </PageTransition>
    );
  }

  function renderColumnGroup(title: string, groupColumns: ColumnRow[]) {
    if (groupColumns.length === 0) return null;

    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {groupColumns.map((col) => (
              <div
                key={col.key}
                className="flex items-center justify-between gap-4 rounded-lg border p-3"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-sm font-medium truncate">{col.name}</span>
                  <StatusBadge status={col.status} />
                </div>
                <Select
                  value={col.status}
                  onValueChange={(value) => handleStatusChange(col.key, value as ColumnStatus)}
                >
                  <SelectTrigger className="w-[140px]">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="default">Normal</SelectItem>
                    <SelectItem value="ignore">Ignorar</SelectItem>
                    <SelectItem value="important">Importante</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <PageTransition>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-2">
              <Bell className="h-6 w-6" />
              Configuracion de Alertas de Columnas
            </h1>
            <p className="text-muted-foreground text-sm mt-1">
              Configure el comportamiento de alertas para cada columna del archivo MINEDUC
            </p>
          </div>
          <Button onClick={handleSave} disabled={isSaving}>
            {isSaving ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Save className="mr-2 h-4 w-4" />
            )}
            Guardar cambios
          </Button>
        </div>

        {renderColumnGroup("Columnas Criticas", criticalColumns)}
        {renderColumnGroup("Columnas Informativas", infoColumns)}
        {renderColumnGroup("Nuevas", newColumns)}
      </div>
    </PageTransition>
  );
}
