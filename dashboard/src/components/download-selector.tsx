"use client";

import { useState, useCallback } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Download, FileSpreadsheet, FileText, Loader2 } from "lucide-react";
import {
  downloadSEP,
  downloadPIE,
  downloadBRP,
  downloadCombo,
  downloadWord,
} from "@/lib/api";

interface DownloadSelectorProps {
  sessionId: string;
  className?: string;
}

interface DownloadOption {
  key: string;
  label: string;
  icon: typeof FileSpreadsheet;
  downloadFn: (sessionId: string) => Promise<void>;
}

const DOWNLOAD_OPTIONS: DownloadOption[] = [
  {
    key: "sep",
    label: "SEP Procesado",
    icon: FileSpreadsheet,
    downloadFn: downloadSEP,
  },
  {
    key: "pie",
    label: "NORMAL/PIE Procesado",
    icon: FileSpreadsheet,
    downloadFn: downloadPIE,
  },
  {
    key: "brp",
    label: "BRP Distribuido",
    icon: FileSpreadsheet,
    downloadFn: downloadBRP,
  },
  {
    key: "combo",
    label: "Excel Combinado",
    icon: FileSpreadsheet,
    downloadFn: downloadCombo,
  },
  {
    key: "word",
    label: "Informe Word",
    icon: FileText,
    downloadFn: downloadWord,
  },
];

export function DownloadSelector({ sessionId, className }: DownloadSelectorProps) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState<Set<string>>(new Set());
  const [error, setError] = useState<string | null>(null);

  const toggleSelection = useCallback((key: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }, []);

  const handleDownload = useCallback(async () => {
    if (selected.size === 0) return;
    setError(null);

    const optionsToDownload = DOWNLOAD_OPTIONS.filter((opt) =>
      selected.has(opt.key)
    );

    const errors: string[] = [];

    for (const option of optionsToDownload) {
      setLoading((prev) => new Set(prev).add(option.key));

      try {
        await option.downloadFn(sessionId);
      } catch (err) {
        const message =
          err instanceof Error ? err.message : "Error desconocido";
        errors.push(`${option.label}: ${message}`);
      } finally {
        setLoading((prev) => {
          const next = new Set(prev);
          next.delete(option.key);
          return next;
        });
      }
    }

    if (errors.length > 0) {
      setError(errors.join(". "));
    }
  }, [selected, sessionId]);

  const isDownloading = loading.size > 0;
  const hasSelection = selected.size > 0;

  return (
    <Card className={className}>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Download className="h-5 w-5" />
          Descargar Archivos
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {DOWNLOAD_OPTIONS.map((option) => {
            const Icon = option.icon;
            const isLoading = loading.has(option.key);
            const isChecked = selected.has(option.key);

            return (
              <label
                key={option.key}
                className="flex cursor-pointer items-center gap-3 rounded-lg border p-3 transition-colors hover:bg-accent"
              >
                <Checkbox
                  checked={isChecked}
                  onCheckedChange={() => toggleSelection(option.key)}
                  disabled={isDownloading}
                />
                <div className="flex flex-1 items-center gap-2">
                  {isLoading ? (
                    <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
                  ) : (
                    <Icon className="h-4 w-4 text-muted-foreground" />
                  )}
                  <span className="text-sm font-medium">{option.label}</span>
                </div>
                {isLoading && (
                  <span className="text-xs text-muted-foreground">
                    Descargando...
                  </span>
                )}
              </label>
            );
          })}
        </div>

        {error && (
          <Alert variant="destructive" className="mt-4">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <Button
          className="mt-4 w-full"
          onClick={handleDownload}
          disabled={!hasSelection || isDownloading}
        >
          {isDownloading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Descargando...
            </>
          ) : (
            <>
              <Download className="mr-2 h-4 w-4" />
              Descargar seleccionados
              {hasSelection && ` (${selected.size})`}
            </>
          )}
        </Button>
      </CardContent>
    </Card>
  );
}
