"use client";

import { useState, useMemo } from "react";
import { motion } from "framer-motion";
import { Search, AlertCircle, AlertTriangle, Info } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { AuditEntry } from "@/lib/api";

interface AuditLogTableProps {
  data: AuditEntry[];
}

function getNivelIcon(nivel: string) {
  switch (nivel) {
    case "ERROR":
      return <AlertCircle className="h-4 w-4 text-red-500" />;
    case "WARNING":
      return <AlertTriangle className="h-4 w-4 text-amber-500" />;
    default:
      return <Info className="h-4 w-4 text-blue-500" />;
  }
}

function getNivelBadgeVariant(nivel: string) {
  switch (nivel) {
    case "ERROR":
      return "error" as const;
    case "WARNING":
      return "warning" as const;
    default:
      return "info" as const;
  }
}

function formatTimestamp(ts: string): string {
  try {
    const date = new Date(ts);
    return date.toLocaleString("es-CL", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return ts;
  }
}

export function AuditLogTable({ data }: AuditLogTableProps) {
  const [nivelFilter, setNivelFilter] = useState<string>("all");
  const [tipoFilter, setTipoFilter] = useState<string>("all");
  const [searchQuery, setSearchQuery] = useState("");

  const uniqueTipos = useMemo(() => {
    const tipos = new Set(data.map((e) => e.tipo));
    return Array.from(tipos).sort();
  }, [data]);

  const filteredData = useMemo(() => {
    return data.filter((entry) => {
      if (nivelFilter !== "all" && entry.nivel !== nivelFilter) return false;
      if (tipoFilter !== "all" && entry.tipo !== tipoFilter) return false;
      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        return (
          entry.mensaje.toLowerCase().includes(query) ||
          (entry.detalles && entry.detalles.toLowerCase().includes(query)) ||
          entry.tipo.toLowerCase().includes(query)
        );
      }
      return true;
    });
  }, [data, nivelFilter, tipoFilter, searchQuery]);

  const counts = useMemo(() => {
    return {
      total: data.length,
      info: data.filter((e) => e.nivel === "INFO").length,
      warning: data.filter((e) => e.nivel === "WARNING").length,
      error: data.filter((e) => e.nivel === "ERROR").length,
    };
  }, [data]);

  return (
    <div className="space-y-4">
      {/* Summary badges */}
      <div className="flex flex-wrap gap-3">
        <Badge variant="outline" className="text-sm px-3 py-1">
          Total: {counts.total}
        </Badge>
        <Badge variant="info" className="text-sm px-3 py-1">
          <Info className="mr-1.5 h-3.5 w-3.5" />
          Info: {counts.info}
        </Badge>
        <Badge variant="warning" className="text-sm px-3 py-1">
          <AlertTriangle className="mr-1.5 h-3.5 w-3.5" />
          Advertencia: {counts.warning}
        </Badge>
        <Badge variant="error" className="text-sm px-3 py-1">
          <AlertCircle className="mr-1.5 h-3.5 w-3.5" />
          Error: {counts.error}
        </Badge>
      </div>

      {/* Filters */}
      <motion.div
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.3 }}
      >
        <Card>
          <CardContent className="p-4">
            <div className="flex flex-wrap gap-3 items-end">
              <div className="flex-1 min-w-[200px]">
                <label className="text-xs text-muted-foreground mb-1 block">Buscar</label>
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    placeholder="Buscar en mensajes..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-9"
                  />
                </div>
              </div>
              <div className="w-[150px]">
                <label className="text-xs text-muted-foreground mb-1 block">Nivel</label>
                <Select value={nivelFilter} onValueChange={setNivelFilter}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos</SelectItem>
                    <SelectItem value="INFO">Informacion</SelectItem>
                    <SelectItem value="WARNING">Advertencia</SelectItem>
                    <SelectItem value="ERROR">Error</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="w-[200px]">
                <label className="text-xs text-muted-foreground mb-1 block">Tipo</label>
                <Select value={tipoFilter} onValueChange={setTipoFilter}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos</SelectItem>
                    {uniqueTipos.map((tipo) => (
                      <SelectItem key={tipo} value={tipo}>
                        {tipo}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </CardContent>
        </Card>
      </motion.div>

      {/* Table */}
      <Card>
        <CardContent className="p-0">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[50px]"></TableHead>
                <TableHead className="w-[80px]">Nivel</TableHead>
                <TableHead className="w-[170px]">Fecha/Hora</TableHead>
                <TableHead className="w-[180px]">Tipo</TableHead>
                <TableHead>Mensaje</TableHead>
                <TableHead>Detalles</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredData.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={6} className="h-24 text-center text-muted-foreground">
                    No se encontraron registros
                  </TableCell>
                </TableRow>
              ) : (
                filteredData.map((entry, index) => (
                  <motion.tr
                    key={index}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ delay: Math.min(index * 0.02, 0.5) }}
                    className="border-b transition-colors hover:bg-muted/50"
                  >
                    <TableCell>{getNivelIcon(entry.nivel)}</TableCell>
                    <TableCell>
                      <Badge variant={getNivelBadgeVariant(entry.nivel)}>
                        {entry.nivel}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-xs font-mono text-muted-foreground">
                      {formatTimestamp(entry.timestamp)}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline" className="text-[10px] font-mono">
                        {entry.tipo}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-sm">{entry.mensaje}</TableCell>
                    <TableCell className="text-xs text-muted-foreground max-w-[300px] truncate">
                      {entry.detalles || "-"}
                    </TableCell>
                  </motion.tr>
                ))
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <p className="text-xs text-muted-foreground text-center">
        Mostrando {filteredData.length} de {data.length} registros
      </p>
    </div>
  );
}
