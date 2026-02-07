"use client";

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div style={{ padding: 40, fontFamily: "monospace" }}>
      <h2 style={{ color: "#ff6b6b" }}>Error</h2>
      <pre style={{ background: "#1e1e2e", color: "#cdd6f4", padding: 20, borderRadius: 8, overflowX: "auto", whiteSpace: "pre-wrap" }}>
        {error.message}
      </pre>
      <pre style={{ background: "#1e1e2e", color: "#a6adc8", padding: 20, borderRadius: 8, overflowX: "auto", whiteSpace: "pre-wrap", fontSize: 11, marginTop: 10 }}>
        {error.stack}
      </pre>
      <button
        onClick={reset}
        style={{ marginTop: 20, padding: "10px 20px", background: "#4361ee", color: "#fff", border: "none", borderRadius: 8, cursor: "pointer" }}
      >
        Reintentar
      </button>
    </div>
  );
}
