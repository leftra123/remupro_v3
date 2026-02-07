const { app, BrowserWindow, dialog } = require("electron");
const path = require("path");
const { spawn } = require("child_process");
const net = require("net");
const { autoUpdater } = require("electron-updater");

// Paths
const ROOT_DIR = path.join(__dirname, "..");
const VENV_PYTHON = path.join(ROOT_DIR, ".venv", "bin", "python");
const VENV_PYTHON_WIN = path.join(ROOT_DIR, ".venv", "Scripts", "python.exe");
const DASHBOARD_DIR = path.join(ROOT_DIR, "dashboard", "out");
const API_PORT = 8000;

let apiProcess = null;
let mainWindow = null;

function getPythonPath() {
  const fs = require("fs");
  if (process.platform === "win32" && fs.existsSync(VENV_PYTHON_WIN)) {
    return VENV_PYTHON_WIN;
  }
  if (fs.existsSync(VENV_PYTHON)) {
    return VENV_PYTHON;
  }
  return "python";
}

function waitForPort(port, host = "127.0.0.1", timeout = 30000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();

    function tryConnect() {
      if (Date.now() - start > timeout) {
        reject(new Error(`Timeout esperando puerto ${port}`));
        return;
      }

      const socket = new net.Socket();
      socket.setTimeout(1000);

      socket.on("connect", () => {
        socket.destroy();
        resolve();
      });

      socket.on("error", () => {
        socket.destroy();
        setTimeout(tryConnect, 500);
      });

      socket.on("timeout", () => {
        socket.destroy();
        setTimeout(tryConnect, 500);
      });

      socket.connect(port, host);
    }

    tryConnect();
  });
}

function startAPI() {
  const pythonPath = getPythonPath();
  console.log(`Iniciando API con: ${pythonPath}`);

  apiProcess = spawn(
    pythonPath,
    ["-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", String(API_PORT)],
    {
      cwd: ROOT_DIR,
      stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, PYTHONUNBUFFERED: "1" },
    }
  );

  apiProcess.stdout.on("data", (data) => {
    console.log(`[API] ${data.toString().trim()}`);
  });

  apiProcess.stderr.on("data", (data) => {
    console.error(`[API] ${data.toString().trim()}`);
  });

  apiProcess.on("error", (err) => {
    console.error("Error al iniciar API:", err.message);
    dialog.showErrorBox(
      "Error de API",
      `No se pudo iniciar el servidor API.\n\n${err.message}`
    );
  });

  apiProcess.on("exit", (code) => {
    console.log(`API terminado con codigo ${code}`);
    apiProcess = null;
  });
}

function stopAPI() {
  if (apiProcess) {
    console.log("Deteniendo API...");
    if (process.platform === "win32") {
      spawn("taskkill", ["/pid", String(apiProcess.pid), "/f", "/t"]);
    } else {
      apiProcess.kill("SIGTERM");
    }
    apiProcess = null;
  }
}

async function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1024,
    minHeight: 700,
    title: "RemuPro v3",
    icon: path.join(__dirname, "icon.png"),
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  // Load the static Next.js export
  const indexPath = path.join(DASHBOARD_DIR, "index.html");
  const fs = require("fs");

  if (fs.existsSync(indexPath)) {
    mainWindow.loadFile(indexPath);
  } else {
    // Fallback: try dev server
    mainWindow.loadURL("http://localhost:3000");
  }

  // Intercept navigation to absolute paths (e.g. file:///multi-establecimiento)
  // and resolve them to the correct HTML files in dashboard/out/
  mainWindow.webContents.on("will-navigate", (event, url) => {
    if (!url.startsWith("file:///")) return;
    // Ignore if already pointing to our dashboard directory
    if (url.includes(DASHBOARD_DIR)) return;

    event.preventDefault();
    const urlPath = new URL(url).pathname; // e.g. "/multi-establecimiento"

    // Try pageName.html first, then pageName/index.html, then fallback to index.html
    const candidates = [
      path.join(DASHBOARD_DIR, urlPath + ".html"),
      path.join(DASHBOARD_DIR, urlPath, "index.html"),
      path.join(DASHBOARD_DIR, "index.html"),
    ];

    for (const candidate of candidates) {
      // Prevent path traversal: ensure resolved path stays within DASHBOARD_DIR
      const resolved = path.resolve(candidate);
      if (!resolved.startsWith(path.resolve(DASHBOARD_DIR))) continue;
      if (fs.existsSync(resolved)) {
        mainWindow.loadFile(resolved);
        return;
      }
    }
    mainWindow.loadFile(indexPath);
  });

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

// ---------------------------------------------------------------------------
// Auto-Update
// ---------------------------------------------------------------------------

function setupAutoUpdater() {
  autoUpdater.autoDownload = true;
  autoUpdater.autoInstallOnAppQuit = true;

  autoUpdater.on("update-available", (info) => {
    console.log("Update available:", info.version);
  });

  autoUpdater.on("update-downloaded", (info) => {
    const response = dialog.showMessageBoxSync(mainWindow, {
      type: "info",
      title: "Actualizacion disponible",
      message: `Se descargo la version ${info.version}`,
      detail: info.releaseNotes
        ? String(info.releaseNotes).substring(0, 500)
        : "Nueva version lista para instalar.",
      buttons: ["Reiniciar ahora", "Mas tarde"],
      defaultId: 0,
      cancelId: 1,
    });

    if (response === 0) {
      autoUpdater.quitAndInstall();
    }
  });

  autoUpdater.on("error", (err) => {
    console.log("Auto-updater error (non-fatal):", err.message);
  });

  // Check on startup
  autoUpdater.checkForUpdates().catch(() => {});

  // Check every 4 hours
  setInterval(() => {
    autoUpdater.checkForUpdates().catch(() => {});
  }, 4 * 60 * 60 * 1000);
}

app.whenReady().then(async () => {
  // Start FastAPI backend
  startAPI();

  try {
    await waitForPort(API_PORT);
    console.log("API lista en puerto", API_PORT);
  } catch {
    console.error("API no respondio a tiempo");
    dialog.showErrorBox(
      "Error",
      "El servidor API no inicio a tiempo. Verifique que Python y las dependencias estan instaladas."
    );
  }

  await createWindow();

  // Setup auto-updater after window is ready
  setupAutoUpdater();

  app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
});

app.on("window-all-closed", () => {
  stopAPI();
  if (process.platform !== "darwin") {
    app.quit();
  }
});

app.on("before-quit", () => {
  stopAPI();
});
