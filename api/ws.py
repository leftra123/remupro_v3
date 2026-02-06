"""
WebSocket endpoint for real-time progress updates.

Clients connect to /api/ws/progress/{session_id} and receive JSON messages
with progress updates as processing runs.
"""

import asyncio
import json
import logging
from typing import Dict, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from api.session_store import store

logger = logging.getLogger("api.ws")

router = APIRouter(tags=["websocket"])

# Active WebSocket connections grouped by session_id
_connections: Dict[str, Set[WebSocket]] = {}
_lock = asyncio.Lock()


async def _register(session_id: str, ws: WebSocket) -> None:
    async with _lock:
        if session_id not in _connections:
            _connections[session_id] = set()
        _connections[session_id].add(ws)


async def _unregister(session_id: str, ws: WebSocket) -> None:
    async with _lock:
        conns = _connections.get(session_id)
        if conns:
            conns.discard(ws)
            if not conns:
                del _connections[session_id]


async def notify_progress(session_id: str, progress: int, message: str) -> None:
    """
    Send a progress update to all WebSocket clients subscribed to a session.

    Called from the processing background tasks (fire-and-forget).
    """
    async with _lock:
        conns = _connections.get(session_id)
        if not conns:
            return
        # Copy to avoid mutation during iteration
        conns_copy = list(conns)

    payload = json.dumps({
        "session_id": session_id,
        "progress": progress,
        "message": message,
        "status": "processing" if progress < 100 else "completed",
    })

    dead = []
    for ws in conns_copy:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)

    # Cleanup dead connections
    if dead:
        async with _lock:
            conns = _connections.get(session_id)
            if conns:
                for ws in dead:
                    conns.discard(ws)


@router.websocket("/api/ws/progress/{session_id}")
async def websocket_progress(websocket: WebSocket, session_id: str):
    """
    WebSocket endpoint for real-time progress updates.

    Connect to receive JSON messages with:
    - session_id: str
    - progress: int (0-100)
    - message: str
    - status: "processing" | "completed" | "failed"

    The connection stays open until the client disconnects or processing completes.
    """
    await websocket.accept()
    await _register(session_id, websocket)

    logger.info("WebSocket connected for session %s", session_id)

    try:
        # Send initial status
        session = store.get_session(session_id)
        if session:
            await websocket.send_text(json.dumps({
                "session_id": session_id,
                "progress": session.progress,
                "message": session.progress_message or "Connected",
                "status": session.status.value,
            }))

        # Keep connection alive, listening for client messages (ping/pong)
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                # Client can send "ping" to keep alive
                if data == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                # Send heartbeat
                try:
                    session = store.get_session(session_id)
                    if session:
                        await websocket.send_text(json.dumps({
                            "session_id": session_id,
                            "progress": session.progress,
                            "message": session.progress_message or "",
                            "status": session.status.value,
                        }))

                        # Close if processing is done
                        if session.status.value in ("completed", "failed"):
                            await websocket.close()
                            break
                except Exception:
                    break

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected for session %s", session_id)
    except Exception as exc:
        logger.warning("WebSocket error for session %s: %s", session_id, exc)
    finally:
        await _unregister(session_id, websocket)
