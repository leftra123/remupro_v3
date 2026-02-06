"use client";

import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ChevronDown, ChevronRight, AlertTriangle, Building2 } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { EmptyState } from "@/components/motion/empty-state";
import { formatCLP, formatRUT, formatNumber } from "@/lib/utils";
import type { MultiEstablishmentRecord } from "@/lib/api";

interface MultiEstablishmentTableProps {
  data: MultiEstablishmentRecord[];
}

export function MultiEstablishmentTable({ data }: MultiEstablishmentTableProps) {
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  const toggleRow = (rut: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev);
      if (next.has(rut)) {
        next.delete(rut);
      } else {
        next.add(rut);
      }
      return next;
    });
  };

  const expandAll = () => {
    setExpandedRows(new Set(data.map((d) => d.rut)));
  };

  const collapseAll = () => {
    setExpandedRows(new Set());
  };

  if (data.length === 0) {
    return (
      <Card>
        <CardContent className="p-8 text-center">
          <EmptyState
            icon={Building2}
            title="No hay docentes multi-establecimiento"
            description="Todos los docentes trabajan en un solo establecimiento"
          />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-lg">Docentes Multi-Establecimiento</CardTitle>
            <CardDescription>
              {data.length} docente{data.length !== 1 ? "s" : ""} trabajando en 2 o mas establecimientos
            </CardDescription>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" size="sm" onClick={expandAll}>
              Expandir todo
            </Button>
            <Button variant="outline" size="sm" onClick={collapseAll}>
              Colapsar todo
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10"></TableHead>
              <TableHead>RUT</TableHead>
              <TableHead>Nombre</TableHead>
              <TableHead className="text-center">Establec.</TableHead>
              <TableHead className="text-right">Horas Totales</TableHead>
              <TableHead className="text-right">BRP Total</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.map((record) => {
              const isExpanded = expandedRows.has(record.rut);
              const exceedsLimit = record.total_horas > 44;

              return (
                <React.Fragment key={record.rut}>
                  <TableRow
                    className="cursor-pointer transition-colors hover:bg-muted/50"
                    onClick={() => toggleRow(record.rut)}
                  >
                    <TableCell>
                      <Button variant="ghost" size="icon" className="h-6 w-6">
                        <motion.div
                          animate={{ rotate: isExpanded ? 90 : 0 }}
                          transition={{ duration: 0.2 }}
                        >
                          <ChevronRight className="h-4 w-4" />
                        </motion.div>
                      </Button>
                    </TableCell>
                    <TableCell className="font-mono text-sm">{formatRUT(record.rut)}</TableCell>
                    <TableCell className="font-medium">{record.nombre}</TableCell>
                    <TableCell className="text-center">
                      <Badge variant="secondary">{record.establecimientos.length}</Badge>
                    </TableCell>
                    <TableCell className="text-right">
                      <span className={exceedsLimit ? "text-amber-500 font-bold" : ""}>
                        {record.total_horas}
                      </span>
                      {exceedsLimit && (
                        <AlertTriangle className="inline-block ml-1 h-3.5 w-3.5 text-amber-500" />
                      )}
                    </TableCell>
                    <TableCell className="text-right font-bold">{formatCLP(record.total_brp)}</TableCell>
                  </TableRow>
                  <AnimatePresence>
                    {isExpanded && (
                      <TableRow>
                        <TableCell colSpan={6} className="p-0">
                          <motion.div
                            initial={{ opacity: 0, height: 0 }}
                            animate={{ opacity: 1, height: "auto" }}
                            exit={{ opacity: 0, height: 0 }}
                            transition={{ duration: 0.25, ease: "easeOut" }}
                          >
                            <div className="bg-muted/30 p-4 mx-4 mb-3 rounded-lg">
                              <Table>
                                <TableHeader>
                                  <TableRow>
                                    <TableHead>RBD</TableHead>
                                    <TableHead>Escuela</TableHead>
                                    <TableHead className="text-right">Horas</TableHead>
                                    <TableHead className="text-right">Reconocimiento</TableHead>
                                    <TableHead className="text-right">Tramo</TableHead>
                                    <TableHead className="text-right">Prioritarios</TableHead>
                                    <TableHead className="text-right">BRP Total</TableHead>
                                  </TableRow>
                                </TableHeader>
                                <TableBody>
                                  {record.establecimientos.map((est, i) => {
                                    const otherEst = record.establecimientos.filter((_, j) => j !== i);
                                    const hasDiffRecon = otherEst.some(
                                      (o) => Math.abs(o.reconocimiento / (o.horas || 1) - est.reconocimiento / (est.horas || 1)) > 100
                                    );
                                    const hasDiffTramo = otherEst.some(
                                      (o) => Math.abs(o.tramo / (o.horas || 1) - est.tramo / (est.horas || 1)) > 100
                                    );

                                    return (
                                      <TableRow key={est.rbd}>
                                        <TableCell className="font-mono">{est.rbd}</TableCell>
                                        <TableCell>{est.escuela}</TableCell>
                                        <TableCell className="text-right">{est.horas}</TableCell>
                                        <TableCell className={`text-right ${hasDiffRecon ? "text-amber-500 font-medium" : ""}`}>
                                          {formatCLP(est.reconocimiento)}
                                          {hasDiffRecon && <AlertTriangle className="inline-block ml-1 h-3 w-3" />}
                                        </TableCell>
                                        <TableCell className={`text-right ${hasDiffTramo ? "text-amber-500 font-medium" : ""}`}>
                                          {formatCLP(est.tramo)}
                                          {hasDiffTramo && <AlertTriangle className="inline-block ml-1 h-3 w-3" />}
                                        </TableCell>
                                        <TableCell className="text-right">{formatCLP(est.prioritarios)}</TableCell>
                                        <TableCell className="text-right font-bold">{formatCLP(est.brp_total)}</TableCell>
                                      </TableRow>
                                    );
                                  })}
                                </TableBody>
                              </Table>
                            </div>
                          </motion.div>
                        </TableCell>
                      </TableRow>
                    )}
                  </AnimatePresence>
                </React.Fragment>
              );
            })}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}
