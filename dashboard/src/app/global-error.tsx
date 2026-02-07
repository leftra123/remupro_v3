"use client";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <html>
      <body style={{ padding: 40, fontFamily: "monospace", background: "#1a1a2e", color: "#eee" }}>
        <h1 style={{ color: "#ff6b6b" }}>Error en la aplicacion</h1>
        <pre style={{ background: "#16213e", padding: 20, borderRadius: 8, overflowX: "auto", whiteSpace: "pre-wrap" }}>
          {error.message}
        </pre>
        <pre style={{ background: "#16213e", padding: 20, borderRadius: 8, overflowX: "auto", whiteSpace: "pre-wrap", fontSize: 12, marginTop: 10 }}>
          {error.stack}
        </pre>
        <button
          onClick={reset}
          style={{ marginTop: 20, padding: "10px 20px", background: "#4361ee", color: "#fff", border: "none", borderRadius: 8, cursor: "pointer" }}
        >
          Reintentar
        </button>
      </body>
    </html>
  );
}
