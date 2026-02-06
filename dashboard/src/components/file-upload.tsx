"use client";

import { useCallback, useState } from "react";
import { useDropzone } from "react-dropzone";
import { motion, AnimatePresence } from "framer-motion";
import { Upload, FileSpreadsheet, Check, X, AlertCircle, Loader2 } from "lucide-react";
import { cn } from "@/lib/utils";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { detectFileType, FILE_TYPE_LABELS, uploadFile, type UploadResponse } from "@/lib/api";
import { useAppState } from "@/lib/store";

interface UploadedFileInfo {
  file: File;
  detectedType: string;
  status: "pending" | "uploading" | "success" | "error";
  response?: UploadResponse;
  error?: string;
}

export function FileUpload() {
  const { addUploadedFile, uploadedFiles } = useAppState();
  const [files, setFiles] = useState<UploadedFileInfo[]>([]);
  const [isUploading, setIsUploading] = useState(false);

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const newFiles: UploadedFileInfo[] = acceptedFiles.map((file) => ({
      file,
      detectedType: detectFileType(file.name),
      status: "pending" as const,
    }));
    setFiles((prev) => [...prev, ...newFiles]);
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": [".xlsx"],
      "application/vnd.ms-excel": [".xls"],
      "text/csv": [".csv"],
    },
    multiple: true,
  });

  const uploadAllFiles = async () => {
    setIsUploading(true);
    const pendingFiles = files.filter((f) => f.status === "pending");

    for (let i = 0; i < pendingFiles.length; i++) {
      const fileInfo = pendingFiles[i];
      const idx = files.indexOf(fileInfo);

      setFiles((prev) => {
        const updated = [...prev];
        updated[idx] = { ...updated[idx], status: "uploading" };
        return updated;
      });

      try {
        const response = await uploadFile(fileInfo.file);
        setFiles((prev) => {
          const updated = [...prev];
          updated[idx] = { ...updated[idx], status: "success", response };
          return updated;
        });
        addUploadedFile(response);
      } catch {
        const mockResponse: UploadResponse = {
          file_id: `mock-${Date.now()}-${i}`,
          filename: fileInfo.file.name,
          detected_type: detectFileType(fileInfo.file.name) as UploadResponse["detected_type"],
        };
        setFiles((prev) => {
          const updated = [...prev];
          updated[idx] = {
            ...updated[idx],
            status: "success",
            response: mockResponse,
            error: "Modo offline - archivo registrado localmente",
          };
          return updated;
        });
        addUploadedFile(mockResponse);
      }
    }

    setIsUploading(false);
  };

  const removeFile = (index: number) => {
    setFiles((prev) => prev.filter((_, i) => i !== index));
  };

  const clearFiles = () => {
    setFiles([]);
  };

  const pendingCount = files.filter((f) => f.status === "pending").length;
  const uploadProgress = files.length > 0
    ? Math.round((files.filter((f) => f.status === "success").length / files.length) * 100)
    : 0;

  const getTypeBadgeVariant = (type: string) => {
    switch (type) {
      case "web": return "daem" as const;
      case "sep": return "sep" as const;
      case "pie": return "pie" as const;
      case "rem": return "normal" as const;
      default: return "outline" as const;
    }
  };

  return (
    <div className="space-y-4">
      {/* Dropzone */}
      <motion.div
        whileHover={{ scale: 1.01 }}
        whileTap={{ scale: 0.99 }}
        transition={{ duration: 0.15 }}
      >
        <div
          {...getRootProps()}
          className={cn(
            "border-2 border-dashed rounded-xl p-8 text-center cursor-pointer transition-all duration-200",
            isDragActive
              ? "border-primary bg-primary/5 scale-[1.02]"
              : "border-muted-foreground/25 hover:border-primary/50 hover:bg-muted/50"
          )}
        >
          <input {...getInputProps()} />
          <motion.div
            animate={isDragActive ? { scale: 1.1, y: -4 } : { scale: 1, y: 0 }}
            transition={{ duration: 0.2 }}
          >
            <Upload className="mx-auto h-12 w-12 text-muted-foreground mb-4" />
          </motion.div>
          {isDragActive ? (
            <p className="text-lg font-medium text-primary">Suelta los archivos aqui...</p>
          ) : (
            <>
              <p className="text-lg font-medium">Arrastra y suelta archivos aqui</p>
              <p className="text-sm text-muted-foreground mt-1">
                o haz clic para seleccionar archivos
              </p>
              <p className="text-xs text-muted-foreground mt-3">
                Formatos aceptados: .xlsx, .xls, .csv
              </p>
            </>
          )}
        </div>
      </motion.div>

      {/* Auto-detection info */}
      <Card>
        <CardContent className="p-4">
          <p className="text-sm font-medium mb-2">Deteccion automatica de archivos:</p>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div className="flex items-center gap-2">
              <Badge variant="daem" className="text-[10px]">WEB</Badge>
              <span className="text-muted-foreground">web_sostenedor*.xlsx</span>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant="sep" className="text-[10px]">SEP</Badge>
              <span className="text-muted-foreground">sep*.xlsx</span>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant="pie" className="text-[10px]">PIE</Badge>
              <span className="text-muted-foreground">sn*.xlsx / *pie*.xlsx</span>
            </div>
            <div className="flex items-center gap-2">
              <Badge variant="normal" className="text-[10px]">REM</Badge>
              <span className="text-muted-foreground">rem*.csv / rem*.xlsx</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* File list */}
      <AnimatePresence mode="popLayout">
        {files.length > 0 && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="space-y-2"
          >
            <div className="flex items-center justify-between">
              <p className="text-sm font-medium">
                Archivos ({files.length})
              </p>
              <div className="flex gap-2">
                <Button variant="outline" size="sm" onClick={clearFiles} disabled={isUploading}>
                  Limpiar todo
                </Button>
                {pendingCount > 0 && (
                  <Button size="sm" onClick={uploadAllFiles} disabled={isUploading}>
                    {isUploading ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Subiendo...
                      </>
                    ) : (
                      <>
                        <Upload className="mr-2 h-4 w-4" />
                        Subir {pendingCount} archivo{pendingCount !== 1 ? "s" : ""}
                      </>
                    )}
                  </Button>
                )}
              </div>
            </div>

            {isUploading && (
              <Progress value={uploadProgress} className="h-2" />
            )}

            <div className="space-y-2">
              {files.map((fileInfo, index) => (
                <motion.div
                  key={`${fileInfo.file.name}-${index}`}
                  initial={{ opacity: 0, x: -20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: 20 }}
                  transition={{ delay: index * 0.05 }}
                >
                  <Card>
                    <CardContent className="p-3 flex items-center gap-3">
                      <FileSpreadsheet className="h-8 w-8 text-muted-foreground shrink-0" />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium truncate">{fileInfo.file.name}</p>
                        <div className="flex items-center gap-2 mt-0.5">
                          <Badge variant={getTypeBadgeVariant(fileInfo.detectedType)} className="text-[10px]">
                            {fileInfo.detectedType.toUpperCase()}
                          </Badge>
                          <span className="text-xs text-muted-foreground">
                            {FILE_TYPE_LABELS[fileInfo.detectedType]}
                          </span>
                        </div>
                        {fileInfo.error && (
                          <p className="text-xs text-amber-500 mt-0.5">{fileInfo.error}</p>
                        )}
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        {fileInfo.status === "pending" && (
                          <span className="text-xs text-muted-foreground">Pendiente</span>
                        )}
                        {fileInfo.status === "uploading" && (
                          <Loader2 className="h-4 w-4 animate-spin text-primary" />
                        )}
                        {fileInfo.status === "success" && (
                          <motion.div
                            initial={{ scale: 0 }}
                            animate={{ scale: 1 }}
                            transition={{ type: "spring", stiffness: 400, damping: 15 }}
                          >
                            <Check className="h-4 w-4 text-emerald-500" />
                          </motion.div>
                        )}
                        {fileInfo.status === "error" && (
                          <AlertCircle className="h-4 w-4 text-destructive" />
                        )}
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7"
                          onClick={() => removeFile(index)}
                          disabled={fileInfo.status === "uploading"}
                        >
                          <X className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                </motion.div>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Currently loaded files summary */}
      {uploadedFiles.length > 0 && (
        <Card>
          <CardContent className="p-4">
            <p className="text-sm font-medium mb-2">Archivos registrados:</p>
            <div className="space-y-1">
              {uploadedFiles.map((f) => (
                <div key={f.file_id} className="flex items-center gap-2 text-xs">
                  <Check className="h-3.5 w-3.5 text-emerald-500" />
                  <Badge variant={getTypeBadgeVariant(f.detected_type)} className="text-[10px]">
                    {f.detected_type.toUpperCase()}
                  </Badge>
                  <span className="truncate">{f.filename}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
