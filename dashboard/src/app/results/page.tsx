"use client";

import { useState, useMemo } from "react";
import { motion } from "framer-motion";
import {
  Search,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  BarChart3,
} from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DownloadSelector } from "@/components/download-selector";
import { PageTransition } from "@/components/motion/page-transition";
import { EmptyState } from "@/components/motion/empty-state";
import { useAppState } from "@/lib/store";
import { formatCLP, formatRUT } from "@/lib/utils";

type SortField =
  | "nombre"
  | "rut"
  | "escuela"
  | "horas_total"
  | "brp_total"
  | "brp_sep"
  | "brp_pie"
  | "brp_normal"
  | "daem"
  | "cpeip";
type SortDir = "asc" | "desc";
type SubsidyFilter = "all" | "sep" | "pie" | "normal";

export default function ResultsPage() {
  const { records, sessionId, summary, loadDemoData } = useAppState();
  const [searchQuery, setSearchQuery] = useState("");
  const [schoolFilter, setSchoolFilter] = useState("all");
  const [subsidyFilter, setSubsidyFilter] = useState<SubsidyFilter>("all");
  const [sortField, setSortField] = useState<SortField>("nombre");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [page, setPage] = useState(0);
  const pageSize = 20;

  const schools = useMemo(() => {
    const s = new Set(records.map((r) => r.escuela));
    return Array.from(s).sort();
  }, [records]);

  const filteredRecords = useMemo(() => {
    let data = [...records];

    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      data = data.filter(
        (r) =>
          r.nombre.toLowerCase().includes(q) ||
          r.rut.toLowerCase().includes(q) ||
          r.escuela.toLowerCase().includes(q)
      );
    }

    if (schoolFilter !== "all") {
      data = data.filter((r) => r.escuela === schoolFilter);
    }

    if (subsidyFilter !== "all") {
      data = data.filter((r) => {
        switch (subsidyFilter) {
          case "sep": return r.brp_sep > 0;
          case "pie": return r.brp_pie > 0;
          case "normal": return r.brp_normal > 0;
          default: return true;
        }
      });
    }

    data.sort((a, b) => {
      const aVal = a[sortField];
      const bVal = b[sortField];
      const cmp = typeof aVal === "string"
        ? (aVal as string).localeCompare(bVal as string)
        : (aVal as number) - (bVal as number);
      return sortDir === "asc" ? cmp : -cmp;
    });

    return data;
  }, [records, searchQuery, schoolFilter, subsidyFilter, sortField, sortDir]);

  const totalPages = Math.ceil(filteredRecords.length / pageSize);
  const paginatedRecords = filteredRecords.slice(page * pageSize, (page + 1) * pageSize);

  const toggleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  };

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return <ArrowUpDown className="h-3 w-3 ml-1 opacity-40" />;
    return sortDir === "asc" ? (
      <ArrowUp className="h-3 w-3 ml-1" />
    ) : (
      <ArrowDown className="h-3 w-3 ml-1" />
    );
  };

  if (!sessionId || records.length === 0) {
    return (
      <PageTransition>
        <EmptyState
          icon={BarChart3}
          title="No hay resultados disponibles"
          description="Suba y procese archivos para ver los resultados de distribucion BRP."
          onDemo={loadDemoData}
        />
      </PageTransition>
    );
  }

  const filteredTotals = filteredRecords.reduce(
    (acc, r) => ({
      brp_total: acc.brp_total + r.brp_total,
      brp_sep: acc.brp_sep + r.brp_sep,
      brp_pie: acc.brp_pie + r.brp_pie,
      brp_normal: acc.brp_normal + r.brp_normal,
      daem: acc.daem + r.daem,
      cpeip: acc.cpeip + r.cpeip,
    }),
    { brp_total: 0, brp_sep: 0, brp_pie: 0, brp_normal: 0, daem: 0, cpeip: 0 }
  );

  return (
    <PageTransition>
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold">Resultados BRP</h1>
            <p className="text-muted-foreground text-sm">
              Tabla completa de distribucion BRP por docente
            </p>
          </div>
        </div>

        {/* Download selector */}
        {sessionId && sessionId !== "demo" && (
          <DownloadSelector sessionId={sessionId} />
        )}

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
                      placeholder="Buscar por nombre, RUT o escuela..."
                      value={searchQuery}
                      onChange={(e) => { setSearchQuery(e.target.value); setPage(0); }}
                      className="pl-9"
                    />
                  </div>
                </div>
                <div className="w-[250px]">
                  <label className="text-xs text-muted-foreground mb-1 block">Establecimiento</label>
                  <Select value={schoolFilter} onValueChange={(v) => { setSchoolFilter(v); setPage(0); }}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Todos</SelectItem>
                      {schools.map((s) => (
                        <SelectItem key={s} value={s}>
                          {s}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="w-[180px]">
                  <label className="text-xs text-muted-foreground mb-1 block">Subvencion</label>
                  <Select value={subsidyFilter} onValueChange={(v) => { setSubsidyFilter(v as SubsidyFilter); setPage(0); }}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">Todas</SelectItem>
                      <SelectItem value="sep">Solo SEP</SelectItem>
                      <SelectItem value="pie">Solo PIE</SelectItem>
                      <SelectItem value="normal">Solo Normal</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </CardContent>
          </Card>
        </motion.div>

        {/* Summary for filtered */}
        <div className="grid grid-cols-3 md:grid-cols-6 gap-2">
          <div className="text-center p-2 rounded-lg bg-muted/50">
            <p className="text-[10px] text-muted-foreground">Total BRP</p>
            <p className="text-sm font-bold">{formatCLP(filteredTotals.brp_total)}</p>
          </div>
          <div className="text-center p-2 rounded-lg bg-blue-500/5">
            <p className="text-[10px] text-blue-500">SEP</p>
            <p className="text-sm font-bold text-blue-600 dark:text-blue-400">{formatCLP(filteredTotals.brp_sep)}</p>
          </div>
          <div className="text-center p-2 rounded-lg bg-emerald-500/5">
            <p className="text-[10px] text-emerald-500">PIE</p>
            <p className="text-sm font-bold text-emerald-600 dark:text-emerald-400">{formatCLP(filteredTotals.brp_pie)}</p>
          </div>
          <div className="text-center p-2 rounded-lg bg-amber-500/5">
            <p className="text-[10px] text-amber-500">Normal</p>
            <p className="text-sm font-bold text-amber-600 dark:text-amber-400">{formatCLP(filteredTotals.brp_normal)}</p>
          </div>
          <div className="text-center p-2 rounded-lg bg-indigo-500/5">
            <p className="text-[10px] text-indigo-500">DAEM</p>
            <p className="text-sm font-bold text-indigo-600 dark:text-indigo-400">{formatCLP(filteredTotals.daem)}</p>
          </div>
          <div className="text-center p-2 rounded-lg bg-pink-500/5">
            <p className="text-[10px] text-pink-500">CPEIP</p>
            <p className="text-sm font-bold text-pink-600 dark:text-pink-400">{formatCLP(filteredTotals.cpeip)}</p>
          </div>
        </div>

        {/* Table */}
        <Card>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("rut")}>
                      <div className="flex items-center">RUT <SortIcon field="rut" /></div>
                    </TableHead>
                    <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("nombre")}>
                      <div className="flex items-center">Nombre <SortIcon field="nombre" /></div>
                    </TableHead>
                    <TableHead className="cursor-pointer select-none" onClick={() => toggleSort("escuela")}>
                      <div className="flex items-center">Escuela <SortIcon field="escuela" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none" onClick={() => toggleSort("horas_total")}>
                      <div className="flex items-center justify-end">Hrs <SortIcon field="horas_total" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none text-blue-500" onClick={() => toggleSort("brp_sep")}>
                      <div className="flex items-center justify-end">BRP SEP <SortIcon field="brp_sep" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none text-emerald-500" onClick={() => toggleSort("brp_pie")}>
                      <div className="flex items-center justify-end">BRP PIE <SortIcon field="brp_pie" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none text-amber-500" onClick={() => toggleSort("brp_normal")}>
                      <div className="flex items-center justify-end">BRP Normal <SortIcon field="brp_normal" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none font-bold" onClick={() => toggleSort("brp_total")}>
                      <div className="flex items-center justify-end">BRP Total <SortIcon field="brp_total" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none text-indigo-500" onClick={() => toggleSort("daem")}>
                      <div className="flex items-center justify-end">DAEM <SortIcon field="daem" /></div>
                    </TableHead>
                    <TableHead className="text-right cursor-pointer select-none text-pink-500" onClick={() => toggleSort("cpeip")}>
                      <div className="flex items-center justify-end">CPEIP <SortIcon field="cpeip" /></div>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {paginatedRecords.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={10} className="h-24 text-center text-muted-foreground">
                        No se encontraron resultados
                      </TableCell>
                    </TableRow>
                  ) : (
                    paginatedRecords.map((r, i) => (
                      <TableRow key={`${r.rut}-${r.rbd}-${i}`} className="transition-colors hover:bg-muted/50">
                        <TableCell className="font-mono text-xs">{formatRUT(r.rut)}</TableCell>
                        <TableCell className="font-medium text-sm max-w-[200px] truncate">{r.nombre}</TableCell>
                        <TableCell className="text-xs text-muted-foreground max-w-[180px] truncate">{r.escuela}</TableCell>
                        <TableCell className="text-right text-sm">{r.horas_total}</TableCell>
                        <TableCell className="text-right text-sm text-blue-600 dark:text-blue-400">{formatCLP(r.brp_sep)}</TableCell>
                        <TableCell className="text-right text-sm text-emerald-600 dark:text-emerald-400">{formatCLP(r.brp_pie)}</TableCell>
                        <TableCell className="text-right text-sm text-amber-600 dark:text-amber-400">{formatCLP(r.brp_normal)}</TableCell>
                        <TableCell className="text-right text-sm font-bold">{formatCLP(r.brp_total)}</TableCell>
                        <TableCell className="text-right text-sm text-indigo-600 dark:text-indigo-400">{formatCLP(r.daem)}</TableCell>
                        <TableCell className="text-right text-sm text-pink-600 dark:text-pink-400">{formatCLP(r.cpeip)}</TableCell>
                      </TableRow>
                    ))
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between">
            <p className="text-sm text-muted-foreground">
              Mostrando {page * pageSize + 1}-{Math.min((page + 1) * pageSize, filteredRecords.length)} de {filteredRecords.length} registros
            </p>
            <div className="flex gap-2">
              <Button
                variant="outline"
                size="sm"
                disabled={page === 0}
                onClick={() => setPage((p) => p - 1)}
              >
                Anterior
              </Button>
              <div className="flex items-center gap-1">
                {Array.from({ length: Math.min(totalPages, 5) }, (_, i) => {
                  const pageNum = page < 3 ? i : page - 2 + i;
                  if (pageNum >= totalPages) return null;
                  return (
                    <Button
                      key={pageNum}
                      variant={pageNum === page ? "default" : "outline"}
                      size="sm"
                      className="w-8"
                      onClick={() => setPage(pageNum)}
                    >
                      {pageNum + 1}
                    </Button>
                  );
                })}
              </div>
              <Button
                variant="outline"
                size="sm"
                disabled={page >= totalPages - 1}
                onClick={() => setPage((p) => p + 1)}
              >
                Siguiente
              </Button>
            </div>
          </div>
        )}
      </div>
    </PageTransition>
  );
}
