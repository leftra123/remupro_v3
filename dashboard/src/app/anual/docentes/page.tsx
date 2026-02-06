"use client";

import { Suspense, useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import { Search, ChevronDown, ChevronUp } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { PageTransition } from "@/components/motion/page-transition";
import {
  searchAnualTeachers,
  getAnualYears,
  getAnualSchools,
  AnualSchoolEntry,
} from "@/lib/api";
import { formatCLP } from "@/lib/utils";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";

interface TeacherRow {
  rut: string;
  nombre: string;
  mes: string;
  tipo_subvencion: string;
  escuela: string;
  rbd: string;
  jornada: number;
  brp: number;
  sueldo_base: number;
  total_haberes: number;
  liquido_neto: number;
  monto_imponible: number;
}

export default function DocentesAnualPage() {
  return (
    <Suspense fallback={<div className="p-8 text-center text-muted-foreground">Cargando...</div>}>
      <DocentesAnualContent />
    </Suspense>
  );
}

function DocentesAnualContent() {
  const searchParams = useSearchParams();
  const anioParam = searchParams.get("anio");

  const [years, setYears] = useState<number[]>([]);
  const [selectedYear, setSelectedYear] = useState<number>(
    anioParam ? parseInt(anioParam) : new Date().getFullYear()
  );
  const [query, setQuery] = useState("");
  const [rbd, setRbd] = useState("");
  const [schools, setSchools] = useState<AnualSchoolEntry[]>([]);
  const [results, setResults] = useState<TeacherRow[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [expandedRut, setExpandedRut] = useState<string | null>(null);
  const limit = 50;

  useEffect(() => {
    getAnualYears().then(setYears).catch(() => {});
  }, []);

  useEffect(() => {
    if (selectedYear) {
      getAnualSchools(selectedYear).then(setSchools).catch(() => {});
    }
  }, [selectedYear]);

  const doSearch = (newOffset = 0) => {
    setLoading(true);
    setOffset(newOffset);
    searchAnualTeachers(selectedYear, query, rbd, limit, newOffset)
      .then((res) => {
        setResults(res.docentes as unknown as TeacherRow[]);
        setTotal(res.total);
      })
      .catch(() => {
        setResults([]);
        setTotal(0);
      })
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (selectedYear) doSearch(0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedYear, rbd]);

  // Agrupar resultados por RUT
  const grouped = results.reduce<Record<string, TeacherRow[]>>((acc, row) => {
    if (!acc[row.rut]) acc[row.rut] = [];
    acc[row.rut].push(row);
    return acc;
  }, {});

  return (
    <PageTransition>
      <div className="space-y-6 max-w-5xl">
        <div>
          <h1 className="text-2xl font-bold">Docentes - Liquidacion Anual</h1>
          <p className="text-muted-foreground text-sm">
            Busqueda de docentes por RUT o nombre en datos anuales
          </p>
        </div>

        {/* Filters */}
        <Card>
          <CardContent className="pt-4 pb-3">
            <div className="flex gap-3 items-end flex-wrap">
              <div className="flex-1 min-w-[200px]">
                <label className="text-sm font-medium mb-1.5 block">Buscar</label>
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    placeholder="RUT o nombre..."
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && doSearch(0)}
                    className="pl-9"
                  />
                </div>
              </div>
              <div className="w-40">
                <label className="text-sm font-medium mb-1.5 block">Ano</label>
                <Select
                  value={selectedYear.toString()}
                  onValueChange={(v) => setSelectedYear(parseInt(v))}
                >
                  <SelectTrigger>
                    <SelectValue />
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
              <div className="w-48">
                <label className="text-sm font-medium mb-1.5 block">Escuela</label>
                <Select value={rbd} onValueChange={setRbd}>
                  <SelectTrigger>
                    <SelectValue placeholder="Todas" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="">Todas</SelectItem>
                    {schools.map((s) => (
                      <SelectItem key={s.rbd} value={s.rbd}>
                        {s.escuela} ({s.rbd})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <Button onClick={() => doSearch(0)} disabled={loading}>
                Buscar
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Results */}
        <div className="space-y-2">
          <p className="text-sm text-muted-foreground">
            {total} registros encontrados
          </p>

          {Object.entries(grouped).map(([rutKey, rows]) => {
            const first = rows[0];
            const totalBrp = rows.reduce((s, r) => s + r.brp, 0);
            const isExpanded = expandedRut === rutKey;
            return (
              <Card key={rutKey}>
                <CardContent className="py-3">
                  <div
                    className="flex items-center justify-between cursor-pointer"
                    onClick={() => setExpandedRut(isExpanded ? null : rutKey)}
                  >
                    <div className="flex items-center gap-3">
                      <div>
                        <p className="font-medium">{first.nombre}</p>
                        <p className="text-xs text-muted-foreground">{first.rut}</p>
                      </div>
                      <Badge variant="outline">{rows.length} registros</Badge>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="text-right">
                        <p className="text-sm font-medium">{formatCLP(totalBrp)}</p>
                        <p className="text-xs text-muted-foreground">BRP Total</p>
                      </div>
                      {isExpanded ? (
                        <ChevronUp className="h-4 w-4" />
                      ) : (
                        <ChevronDown className="h-4 w-4" />
                      )}
                    </div>
                  </div>
                  {isExpanded && (
                    <div className="mt-3 border-t pt-3">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="text-xs text-muted-foreground border-b">
                            <th className="text-left py-1 pr-2">Mes</th>
                            <th className="text-left py-1 pr-2">Tipo</th>
                            <th className="text-left py-1 pr-2">Escuela</th>
                            <th className="text-right py-1 pr-2">BRP</th>
                            <th className="text-right py-1 pr-2">Haberes</th>
                            <th className="text-right py-1">Liquido</th>
                          </tr>
                        </thead>
                        <tbody>
                          {rows.map((r, i) => (
                            <tr key={i} className="border-b last:border-0">
                              <td className="py-1 pr-2">{r.mes}</td>
                              <td className="py-1 pr-2">
                                <Badge variant="outline" className="text-[10px]">
                                  {r.tipo_subvencion}
                                </Badge>
                              </td>
                              <td className="py-1 pr-2 text-xs">{r.escuela}</td>
                              <td className="py-1 pr-2 text-right">{formatCLP(r.brp)}</td>
                              <td className="py-1 pr-2 text-right">{formatCLP(r.total_haberes)}</td>
                              <td className="py-1 text-right">{formatCLP(r.liquido_neto)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </CardContent>
              </Card>
            );
          })}

          {/* Pagination */}
          {total > limit && (
            <div className="flex justify-center gap-3 pt-4">
              <Button
                variant="outline"
                size="sm"
                disabled={offset === 0}
                onClick={() => doSearch(Math.max(0, offset - limit))}
              >
                Anterior
              </Button>
              <span className="text-sm text-muted-foreground self-center">
                {offset + 1} - {Math.min(offset + limit, total)} de {total}
              </span>
              <Button
                variant="outline"
                size="sm"
                disabled={offset + limit >= total}
                onClick={() => doSearch(offset + limit)}
              >
                Siguiente
              </Button>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}
