"use client";

import { useEffect, useRef, useState, useMemo } from "react";
import { motion, AnimatePresence } from "framer-motion";
import * as d3 from "d3";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ArrowLeft } from "lucide-react";
import { formatCLP } from "@/lib/utils";
import type { BRPRecord, SchoolSummary } from "@/lib/api";

interface D3TreemapProps {
  records: BRPRecord[];
  schoolSummary: SchoolSummary[];
  className?: string;
}

interface TreemapNode {
  name: string;
  value?: number;
  children?: TreemapNode[];
  sep?: number;
  pie?: number;
  normal?: number;
  rbd?: number;
}

interface TooltipState {
  x: number;
  y: number;
  name: string;
  value: number;
  sep: number;
  pie: number;
  normal: number;
  visible: boolean;
}

function getSubsidyColor(sep: number, pie: number, normal: number): string {
  const total = sep + pie + normal;
  if (total === 0) return "#6b7280";
  const pSep = sep / total;
  const pPie = pie / total;

  // Blend from blue (SEP) through green (PIE) to amber (Normal)
  const r = Math.round(59 * pSep + 16 * pPie + 245 * (1 - pSep - pPie));
  const g = Math.round(130 * pSep + 185 * pPie + 158 * (1 - pSep - pPie));
  const b = Math.round(246 * pSep + 129 * pPie + 11 * (1 - pSep - pPie));
  return `rgb(${r}, ${g}, ${b})`;
}

export function D3Treemap({ records, schoolSummary, className }: D3TreemapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 600, height: 400 });
  const [selectedSchool, setSelectedSchool] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<TooltipState>({
    x: 0, y: 0, name: "", value: 0, sep: 0, pie: 0, normal: 0, visible: false,
  });

  // Build hierarchical data
  const rootData = useMemo((): TreemapNode => {
    if (selectedSchool) {
      const schoolRecords = records.filter((r) => r.escuela === selectedSchool);
      return {
        name: selectedSchool,
        children: schoolRecords.map((r) => ({
          name: r.nombre,
          value: r.brp_total,
          sep: r.brp_sep,
          pie: r.brp_pie,
          normal: r.brp_normal,
          rbd: r.rbd,
        })),
      };
    }

    return {
      name: "BRP Total",
      children: schoolSummary.map((s) => ({
        name: s.escuela,
        value: s.brp_sep + s.brp_pie + s.brp_normal,
        sep: s.brp_sep,
        pie: s.brp_pie,
        normal: s.brp_normal,
        rbd: s.rbd,
      })),
    };
  }, [records, schoolSummary, selectedSchool]);

  // Observe container size
  useEffect(() => {
    if (!containerRef.current) return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry) {
        setDimensions({
          width: entry.contentRect.width,
          height: Math.max(400, entry.contentRect.width * 0.55),
        });
      }
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  // Compute treemap layout
  const treemapNodes = useMemo(() => {
    const hierarchy = d3
      .hierarchy(rootData)
      .sum((d) => d.value || 0)
      .sort((a, b) => (b.value || 0) - (a.value || 0));

    const treemap = d3
      .treemap<TreemapNode>()
      .size([dimensions.width, dimensions.height])
      .paddingInner(3)
      .paddingOuter(4)
      .round(true);

    const root = treemap(hierarchy);
    return root.leaves() as d3.HierarchyRectangularNode<TreemapNode>[];
  }, [rootData, dimensions]);

  const handleClick = (node: d3.HierarchyRectangularNode<TreemapNode>) => {
    if (!selectedSchool && node.data.name) {
      setSelectedSchool(node.data.name);
    }
  };

  const handleMouseMove = (
    e: React.MouseEvent,
    node: d3.HierarchyRectangularNode<TreemapNode>
  ) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    setTooltip({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top - 10,
      name: node.data.name,
      value: node.value || 0,
      sep: node.data.sep || 0,
      pie: node.data.pie || 0,
      normal: node.data.normal || 0,
      visible: true,
    });
  };

  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {selectedSchool && (
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                onClick={() => setSelectedSchool(null)}
              >
                <ArrowLeft className="h-4 w-4" />
              </Button>
            )}
            <CardTitle className="text-base">
              {selectedSchool ? selectedSchool : "Mapa de Distribucion BRP"}
            </CardTitle>
          </div>
          <div className="flex gap-2">
            <Badge variant="sep" className="text-[10px]">SEP</Badge>
            <Badge variant="pie" className="text-[10px]">PIE</Badge>
            <Badge variant="normal" className="text-[10px]">Normal</Badge>
          </div>
        </div>
        {!selectedSchool && (
          <p className="text-xs text-muted-foreground">Haz clic en una escuela para ver docentes</p>
        )}
      </CardHeader>
      <CardContent>
        <div ref={containerRef} className="relative w-full" style={{ height: dimensions.height }}>
          <svg width={dimensions.width} height={dimensions.height}>
            <AnimatePresence mode="wait">
              {treemapNodes.map((node, i) => {
                const w = (node.x1 || 0) - (node.x0 || 0);
                const h = (node.y1 || 0) - (node.y0 || 0);
                if (w < 2 || h < 2) return null;

                const color = getSubsidyColor(
                  node.data.sep || 0,
                  node.data.pie || 0,
                  node.data.normal || 0
                );

                const showLabel = w > 50 && h > 30;
                const showValue = w > 70 && h > 45;

                return (
                  <motion.g
                    key={`${selectedSchool || "root"}-${node.data.name}-${i}`}
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.3, delay: Math.min(i * 0.03, 0.5) }}
                  >
                    <rect
                      x={node.x0}
                      y={node.y0}
                      width={w}
                      height={h}
                      rx={4}
                      fill={color}
                      fillOpacity={0.85}
                      stroke="hsl(var(--background))"
                      strokeWidth={1.5}
                      className={!selectedSchool ? "cursor-pointer" : ""}
                      onClick={() => handleClick(node)}
                      onMouseMove={(e) => handleMouseMove(e, node)}
                      onMouseLeave={() => setTooltip((t) => ({ ...t, visible: false }))}
                    />
                    {showLabel && (
                      <text
                        x={(node.x0 || 0) + 6}
                        y={(node.y0 || 0) + 16}
                        className="text-[11px] font-medium"
                        fill="white"
                        style={{ pointerEvents: "none" }}
                      >
                        {node.data.name.length > w / 7
                          ? node.data.name.substring(0, Math.floor(w / 7)) + "..."
                          : node.data.name}
                      </text>
                    )}
                    {showValue && (
                      <text
                        x={(node.x0 || 0) + 6}
                        y={(node.y0 || 0) + 32}
                        className="text-[10px]"
                        fill="rgba(255,255,255,0.8)"
                        style={{ pointerEvents: "none" }}
                      >
                        {formatCLP(node.value || 0)}
                      </text>
                    )}
                  </motion.g>
                );
              })}
            </AnimatePresence>
          </svg>

          {/* Tooltip */}
          {tooltip.visible && (
            <div
              className="glass absolute z-50 rounded-xl p-3 shadow-lg border pointer-events-none"
              style={{
                left: Math.min(tooltip.x, dimensions.width - 200),
                top: Math.max(tooltip.y - 90, 0),
              }}
            >
              <p className="font-medium text-sm mb-1">{tooltip.name}</p>
              <p className="text-sm font-bold">{formatCLP(tooltip.value)}</p>
              <div className="mt-1.5 space-y-0.5 text-xs">
                <div className="flex justify-between gap-3">
                  <span className="text-blue-400">SEP</span>
                  <span>{formatCLP(tooltip.sep)}</span>
                </div>
                <div className="flex justify-between gap-3">
                  <span className="text-emerald-400">PIE</span>
                  <span>{formatCLP(tooltip.pie)}</span>
                </div>
                <div className="flex justify-between gap-3">
                  <span className="text-amber-400">Normal</span>
                  <span>{formatCLP(tooltip.normal)}</span>
                </div>
              </div>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
