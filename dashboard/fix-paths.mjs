/**
 * Post-build script: rewrites absolute /_next/ paths to relative paths
 * in static HTML exports so they work under Electron's file:// protocol.
 *
 * For root HTML files:    /_next/ → ./_next/
 * For nested HTML files:  /_next/ → ../_next/ (or ../../_next/ etc.)
 */
import { readdir, readFile, writeFile } from "fs/promises";
import { join, relative } from "path";

const outDir = join(import.meta.dirname, "out");

async function fixHtmlFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      await fixHtmlFiles(fullPath);
    } else if (entry.name.endsWith(".html")) {
      let content = await readFile(fullPath, "utf8");
      const original = content;

      // Calculate relative prefix based on depth from outDir
      const relPath = relative(dir, outDir);
      const prefix = relPath === "" ? "." : relPath;

      // Fix href="/_next/..." and src="/_next/..."
      content = content.replaceAll('"/_next/', `"${prefix}/_next/`);
      // Fix href="/" for root links
      content = content.replaceAll('href="/"', `href="${prefix}/index.html"`);

      if (content !== original) {
        await writeFile(fullPath, content, "utf8");
        console.log(`Fixed: ${fullPath} (prefix: ${prefix})`);
      }
    }
  }
}

fixHtmlFiles(outDir).then(() => console.log("Done fixing asset paths."));
