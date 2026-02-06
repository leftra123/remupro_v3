"use client";

import { useState } from "react";
import { Loader2, Play, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { FileUpload } from "@/components/file-upload";
import { DownloadSelector } from "@/components/download-selector";
import { PageTransition } from "@/components/motion/page-transition";
import { useAppState } from "@/lib/store";
import { processIntegrado } from "@/lib/api";
import { useRouter } from "next/navigation";

function getCurrentYYYYMM(): string {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

export default function UploadPage() {
  const {
    uploadedFiles,
    isProcessing,
    processingProgress,
    processingMessage,
    setProcessing,
    setResults,
    setSessionId,
    sessionId,
    loadDemoData,
  } = useAppState();
  const router = useRouter();
  const [mes, setMes] = useState(getCurrentYYYYMM());
  const [error, setError] = useState<string | null>(null);
  const [processingComplete, setProcessingComplete] = useState(false);

  const webFile = uploadedFiles.find((f) => f.detected_type === "web");
  const sepFile = uploadedFiles.find((f) => f.detected_type === "sep");
  const pieFile = uploadedFiles.find((f) => f.detected_type === "pie");
  const remFile = uploadedFiles.find((f) => f.detected_type === "rem");
  const anualFile = uploadedFiles.find((f) => f.detected_type === "anual");

  const canProcess = webFile && sepFile && pieFile;

  const handleProcess = async () => {
    if (!canProcess) return;
    setError(null);
    setProcessingComplete(false);
    setProcessing(true, 10, "Iniciando procesamiento...");

    try {
      setProcessing(true, 30, "Procesando archivos SEP y NORMAL/PIE...");
      const result = await processIntegrado({
        web_file_id: webFile.file_id,
        sep_file_id: sepFile.file_id,
        pie_file_id: pieFile.file_id,
        rem_file_id: remFile?.file_id,
        mes,
      });

      setProcessing(true, 80, "Generando resultados...");
      setSessionId(result.session_id);
      setResults({
        summary: result.summary,
        records: result.records,
        multiEstablishment: result.multi_establishment,
        auditLog: result.audit_log,
        schoolSummary: result.school_summary,
      });

      setProcessing(true, 100, "Completado!");
      setProcessingComplete(true);
      setTimeout(() => {
        setProcessing(false);
      }, 500);
    } catch (err) {
      setProcessing(false);
      setError(
        err instanceof Error
          ? err.message
          : "Error desconocido al procesar los archivos"
      );
    }
  };

  const handleDemoMode = () => {
    loadDemoData();
    router.push("/");
  };

  return (
    <PageTransition>
      <div className="space-y-6 max-w-4xl">
        <div>
          <h1 className="text-2xl font-bold">Subir Archivos</h1>
          <p className="text-muted-foreground text-sm">
            Cargue los archivos necesarios para procesar la distribucion BRP
          </p>
        </div>

        <FileUpload />

        {/* Processing config */}
        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Configuracion de Procesamiento</CardTitle>
            <CardDescription>Configure los parametros antes de procesar</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="text-sm font-medium mb-1.5 block">Mes de proceso (YYYY-MM)</label>
                <Input
                  type="month"
                  value={mes}
                  onChange={(e) => setMes(e.target.value)}
                  placeholder="2026-01"
                />
              </div>
            </div>

            {/* Required files status */}
            <div>
              <p className="text-sm font-medium mb-2">Archivos requeridos:</p>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                <div className={`flex items-center gap-2 p-2 rounded-lg border ${webFile ? "border-emerald-500/30 bg-emerald-500/5" : "border-muted"}`}>
                  <div className={`h-2.5 w-2.5 rounded-full ${webFile ? "bg-emerald-500" : "bg-muted-foreground/30"}`} />
                  <span className="text-sm">MINEDUC (web*)</span>
                  {webFile && <Badge variant="daem" className="ml-auto text-[10px]">OK</Badge>}
                </div>
                <div className={`flex items-center gap-2 p-2 rounded-lg border ${sepFile ? "border-emerald-500/30 bg-emerald-500/5" : "border-muted"}`}>
                  <div className={`h-2.5 w-2.5 rounded-full ${sepFile ? "bg-emerald-500" : "bg-muted-foreground/30"}`} />
                  <span className="text-sm">SEP (sep*)</span>
                  {sepFile && <Badge variant="sep" className="ml-auto text-[10px]">OK</Badge>}
                </div>
                <div className={`flex items-center gap-2 p-2 rounded-lg border ${pieFile ? "border-emerald-500/30 bg-emerald-500/5" : "border-muted"}`}>
                  <div className={`h-2.5 w-2.5 rounded-full ${pieFile ? "bg-emerald-500" : "bg-muted-foreground/30"}`} />
                  <span className="text-sm">NORMAL/PIE (sn*/pie*)</span>
                  {pieFile && <Badge variant="pie" className="ml-auto text-[10px]">OK</Badge>}
                </div>
              </div>
              <div className={`flex items-center gap-2 p-2 rounded-lg border mt-2 ${remFile ? "border-emerald-500/30 bg-emerald-500/5" : "border-muted"}`}>
                <div className={`h-2.5 w-2.5 rounded-full ${remFile ? "bg-emerald-500" : "bg-muted-foreground/30"}`} />
                <span className="text-sm">REM (rem*) - Opcional</span>
                {remFile && <Badge variant="normal" className="ml-auto text-[10px]">OK</Badge>}
              </div>
              <div className={`flex items-center gap-2 p-2 rounded-lg border mt-2 ${anualFile ? "border-emerald-500/30 bg-emerald-500/5" : "border-muted"}`}>
                <div className={`h-2.5 w-2.5 rounded-full ${anualFile ? "bg-emerald-500" : "bg-muted-foreground/30"}`} />
                <span className="text-sm">Anual (anual*/liquidacion*/consolidado*) - Opcional</span>
                {anualFile && <Badge variant="normal" className="ml-auto text-[10px]">OK</Badge>}
              </div>
            </div>

            {error && (
              <Alert variant="destructive">
                <AlertCircle className="h-4 w-4" />
                <AlertTitle>Error de procesamiento</AlertTitle>
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}

            {isProcessing && (
              <div className="space-y-2">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{processingMessage}</span>
                  <span className="font-medium">{processingProgress}%</span>
                </div>
                <Progress value={processingProgress} />
              </div>
            )}

            <div className="flex gap-3">
              <Button
                onClick={handleProcess}
                disabled={!canProcess || isProcessing}
                className="flex-1"
              >
                {isProcessing ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Procesando...
                  </>
                ) : (
                  <>
                    <Play className="mr-2 h-4 w-4" />
                    Procesar BRP
                  </>
                )}
              </Button>
              <Button variant="outline" onClick={handleDemoMode}>
                Modo Demo
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Download selector after processing */}
        {processingComplete && sessionId && sessionId !== "demo" && (
          <DownloadSelector sessionId={sessionId} />
        )}

        {/* Quick nav after processing */}
        {processingComplete && (
          <div className="flex gap-3">
            <Button variant="outline" onClick={() => router.push("/")}>
              Ver Dashboard
            </Button>
            <Button variant="outline" onClick={() => router.push("/results")}>
              Ver Resultados
            </Button>
          </div>
        )}
      </div>
    </PageTransition>
  );
}
