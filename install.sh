#!/bin/bash

# ==============================================================================
# SCRIPT DE VICTOR PÉREZ - ADMINISTRADOR BHTTP PRO (INSTALADOR MAESTRO)
# ==============================================================================

CYAN="\033[1;36m"
GREEN="\033[1;32m"
YELLOW="\033[1;33m"
RED="\033[1;31m"
MAGENTA="\033[1;35m"
RESET="\033[0m"

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}[!] Error: Este script debe ejecutarse como root (sudo).${RESET}"
    exit 1
fi

INSTALL_DIR="/usr/local/lib/bhttp"
SERVICE_FILE="/etc/systemd/system/bhttp.service"
PY_SCRIPT="$INSTALL_DIR/bhttp-server.py"
MENU_BIN="/usr/local/bin/bhttp-menu"

mostrar_banner() {
    clear
    echo -e "${MAGENTA}============================================================${RESET}"
    echo -e "${CYAN}   ██╗   ██╗██╗ ██████╗████████╗ ██████╗ ██████╗       ${RESET}"
    echo -e "${CYAN}   ██║   ██║██║██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗      ${RESET}"
    echo -e "${CYAN}   ██║   ██║██║██║        ██║   ██║   ██║██████╔╝      ${RESET}"
    echo -e "${CYAN}   ╚██╗ ██╔╝██║██║        ██║   ██║   ██║██╔══██╗      ${RESET}"
    echo -e "${CYAN}    ╚████╔╝ ██║╚██████╗   ██║   ╚██████╔╝██║  ██║      ${RESET}"
    echo -e "${CYAN}     ╚═══╝  ╚═╝ ╚═════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝      ${RESET}"
    echo -e "${YELLOW}             SCRIPT DE VICTOR PÉREZ (BHTTP PRO)             ${RESET}"
    echo -e "${GREEN}          Optimizado para Máxima Estabilidad y Streaming    ${RESET}"
    echo -e "${MAGENTA}============================================================${RESET}"
}

crear_python_script() {
    mkdir -p "$INSTALL_DIR"
    cat > "$PY_SCRIPT" << 'PYEOF'
#!/usr/bin/env python3
# Servidor BHTTP autonomo optimizado para maxima estabilidad y streaming (YouTube / VoIP / DTunnel).
import argparse, asyncio, hashlib, struct, sys, socket

MAGIC = b"BHP1"
LONGPOLL = 3.0

def log(msg):
    sys.stderr.write("[bhttp-pro] %s\n" % msg); sys.stderr.flush()

def keystream(sess, mode, seq, d, n):
    base = hashlib.sha256(sess + bytes([mode]) + seq.to_bytes(8, "big") + bytes([d]))
    out = bytearray(); c = 0
    while len(out) < n:
        h = base.copy(); h.update(c.to_bytes(4, "big")); out += h.digest(); c += 1
    return bytes(out[:n])

def mask(data, sess, mode, seq, d):
    return bytes(a ^ b for a, b in zip(data, keystream(sess, mode, seq, d, len(data))))

def probe_reply(mode, size):
    n = size if (mode == 2 and size >= 10) else 10
    out = bytearray(MAGIC + bytes([1, mode]) + size.to_bytes(4, "big"))
    for i in range(10, n):
        out.append((i * 31) & 255)
    return bytes(out)

def optimize_socket(sock):
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)
    except Exception:
        pass

class Session:
    def __init__(self, sess, backend):
        self.sess = sess
        self.backend = backend
        self.cond = asyncio.Condition()
        self.up_next = 0
        self.up_pending = {}
        self.down_raw = bytearray()
        self.down_chunks = {}
        self.down_assign = 0
        self.eof = False
        self.closed = False
        self.br = None
        self.bw = None

    async def connect(self):
        host, port = self.backend
        self.br, self.bw = await asyncio.open_connection(host, port)
        sock = self.bw.get_extra_info('socket')
        if sock:
            optimize_socket(sock)
        asyncio.create_task(self._reader())

    async def _reader(self):
        try:
            while True:
                data = await self.br.read(131072)
                if not data:
                    break
                async with self.cond:
                    self.down_raw += data
                    self.cond.notify_all()
        except Exception:
            pass
        finally:
            async with self.cond:
                self.eof = True
                self.cond.notify_all()

    async def upload(self, seq, data):
        async with self.cond:
            if data:
                self.up_pending[seq] = data
            while self.up_next in self.up_pending:
                chunk = self.up_pending.pop(self.up_next)
                try:
                    self.bw.write(chunk)
                    await self.bw.drain()
                except Exception:
                    self.closed = True
                self.up_next += 1

    async def download(self, seq, maxlen, deadline):
        if maxlen <= 0:
            maxlen = 65536
        loop = asyncio.get_running_loop()
        async with self.cond:
            while True:
                if seq < self.down_assign:
                    return self.down_chunks.get(seq, b"")
                if seq == self.down_assign:
                    if self.down_raw:
                        take = bytes(self.down_raw[:maxlen]); del self.down_raw[:maxlen]
                        self.down_chunks[self.down_assign] = take
                        self.down_assign += 1
                        self.cond.notify_all()
                        return take
                    if self.eof:
                        self.down_assign += 1
                        self.cond.notify_all()
                        return b""
                if not self.eof and loop.time() < deadline:
                    try:
                        await asyncio.wait_for(self.cond.wait(),
                                               timeout=max(0.005, deadline - loop.time()))
                    except asyncio.TimeoutError:
                        pass
                    continue
                while self.down_assign <= seq:
                    self.down_assign += 1
                self.cond.notify_all()
                return b""

    async def ack(self, seq):
        async with self.cond:
            for k in [k for k in self.down_chunks if k <= seq]:
                del self.down_chunks[k]

    async def close(self):
        async with self.cond:
            self.closed = True
            self.cond.notify_all()
        try:
            self.bw.close()
        except Exception:
            pass

class Server:
    def __init__(self, host, port, backend):
        self.host, self.port, self.backend = host, port, backend
        self.sessions = {}
        self.slock = asyncio.Lock()

    async def get_session(self, sess):
        async with self.slock:
            s = self.sessions.get(sess)
            if s is None or s.closed:
                s = Session(sess, self.backend)
                await s.connect()
                self.sessions[sess] = s
            return s

    async def handle(self, reader, writer):
        sock = writer.get_extra_info('socket')
        if sock:
            optimize_socket(sock)
        try:
            while True:
                hdr = await reader.readexactly(29)
                mode = hdr[0]
                sess = hdr[1:17]
                seq = int.from_bytes(hdr[17:25], "big")
                ln = int.from_bytes(hdr[25:29], "big")
                payload = b""
                if ln and mode in (0, 1, 2, 3):
                    raw = await reader.readexactly(ln)
                    payload = mask(raw, sess, mode, seq, 0)

                if payload[:4] == MAGIC:
                    size = int.from_bytes(payload[6:10], "big") if len(payload) >= 10 else 0
                    pmode = payload[5] if len(payload) >= 6 else mode
                    body = mask(probe_reply(pmode, size), sess, mode, seq, 1)
                    writer.write(bytes([0]) + len(body).to_bytes(4, "big") + body)
                    await writer.drain()
                    continue

                s = await self.get_session(sess)
                if mode == 1:
                    await s.upload(seq, payload)
                    writer.write(bytes([0]) + (0).to_bytes(4, "big"))
                    await writer.drain()
                elif mode == 2:
                    chunk = await s.download(seq, ln if ln > 0 else 65536,
                                             asyncio.get_running_loop().time() + LONGPOLL)
                    self._send_data(writer, sess, mode, seq, chunk)
                    await writer.drain()
                elif mode == 3:
                    if len(payload) >= 6:
                        chunk_size = int.from_bytes(payload[0:4], "big"); count = payload[5]
                    else:
                        chunk_size, count = 65536, 4
                    if chunk_size <= 0: chunk_size = 65536
                    if count <= 0: count = 4
                    deadline = asyncio.get_running_loop().time() + LONGPOLL
                    for i in range(count):
                        chunk = await s.download(seq + i, chunk_size, deadline)
                        self._send_data(writer, sess, mode, seq + i, chunk)
                    await writer.drain()
                elif mode == 4:
                    await s.ack(seq)
                    writer.write(bytes([0]) + (0).to_bytes(4, "big"))
                    await writer.drain()
                else:
                    return
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    def _send_data(self, writer, sess, mode, seq, data):
        real = len(data)
        masked = mask(data, sess, mode, seq, 1) if data else b""
        body = real.to_bytes(4, "big") + masked
        writer.write(bytes([2]) + len(body).to_bytes(4, "big") + body)

    async def serve(self):
        srv = await asyncio.start_server(self.handle, self.host, self.port, backlog=8192)
        async with srv:
            await srv.serve_forever()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--backend-host", default="127.0.0.1")
    ap.add_argument("--backend-port", type=int, default=22)
    a = ap.parse_args()
    asyncio.run(Server(a.host, a.port, (a.backend_host, a.backend_port)).serve())

if __name__ == "__main__":
    main()
PYEOF
    chmod +x "$PY_SCRIPT"
}

obtener_config_actual() {
    if [ -f "$SERVICE_FILE" ]; then
        EXPR=$(grep "ExecStart=" "$SERVICE_FILE")
        PORT=$(echo "$EXPR" | grep -oP '--port \K\d+')
        BHOST=$(echo "$EXPR" | grep -oP '--backend-host \K[\d\.]+')
        BPORT=$(echo "$EXPR" | grep -oP '--backend-port \K\d+')
    fi
    [ -z "$PORT" ] && PORT="8080"
    [ -z "$BHOST" ] && BHOST="127.0.0.1"
    [ -z "$BPORT" ] && BPORT="22"
}

instalar_bhttp() {
    echo -e "\n${YELLOW}[+] Instalando BHTTP Pro...${RESET}"
    crear_python_script
    
    read -p "Ingrese el puerto de escucha [Predeterminado: 8080]: " p_escucha
    p_escucha=${p_escucha:-8080}
    
    read -p "Ingrese el host de backend [Predeterminado: 127.0.0.1]: " b_host
    b_host=${b_host:-127.0.0.1}
    
    read -p "Ingrese el puerto de backend [Predeterminado: 22]: " b_port
    b_port=${b_port:-22}

    cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Servidor BHTTP Autonomo (Victor Pérez)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/bin/python3 $PY_SCRIPT --port $p_escucha --backend-host $b_host --backend-port $b_port
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl enable bhttp
    systemctl restart bhttp
    
    echo -e "${GREEN}[✔] ¡BHTTP instalado y ejecutándose en el puerto $p_escucha!${RESET}"
    read -p "Presione ENTER para continuar..."
}

desinstalar_bhttp() {
    echo -e "\n${RED}[!] Desinstalando BHTTP Pro...${RESET}"
    systemctl stop bhttp 2>/dev/null
    systemctl disable bhttp 2>/dev/null
    rm -f "$SERVICE_FILE"
    rm -rf "$INSTALL_DIR"
    systemctl daemon-reload
    echo -e "${GREEN}[✔] BHTTP ha sido desinstalado completamente.${RESET}"
    read -p "Presione ENTER para continuar..."
}

cambiar_puerto() {
    if [ ! -f "$SERVICE_FILE" ]; then
        echo -e "${RED}[!] BHTTP no está instalado.${RESET}"
        read -p "Presione ENTER para continuar..."
        return
    fi
    obtener_config_actual
    echo -e "\n${YELLOW}Puerto de escucha actual:${RESET} $PORT"
    read -p "Ingrese el nuevo puerto de escucha: " nuevo_puerto
    if [[ ! "$nuevo_puerto" =~ ^[0-9]+$ ]]; then
        echo -e "${RED}[!] Puerto inválido.${RESET}"
        read -p "Presione ENTER para continuar..."
        return
    fi
    
    sed -i "s/--port [0-9]*/--port $nuevo_puerto/g" "$SERVICE_FILE"
    systemctl daemon-reload
    systemctl restart bhttp
    echo -e "${GREEN}[✔] Puerto cambiado exitosamente a $nuevo_puerto.${RESET}"
    read -p "Presione ENTER para continuar..."
}

editar_bufer() {
    if [ ! -f "$PY_SCRIPT" ]; then
        echo -e "${RED}[!] BHTTP no está instalado.${RESET}"
        read -p "Presione ENTER para continuar..."
        return
    fi
    ACTUAL_LP=$(grep "^LONGPOLL =" "$PY_SCRIPT" | cut -d'=' -f2 | tr -d ' ')
    echo -e "\n${YELLOW}Valor de Búfer / Longpoll actual:${RESET} ${ACTUAL_LP:-3.0}s"
    read -p "Ingrese el nuevo valor para LONGPOLL (ej. 1.0, 3.0, 5.0): " nuevo_lp
    if [[ ! "$nuevo_lp" =~ ^[0-9]+([.,][0-9]+)?$ ]]; then
        echo -e "${RED}[!] Valor inválido.${RESET}"
        read -p "Presione ENTER para continuar..."
        return
    fi
    nuevo_lp=$(echo "$nuevo_lp" | tr ',' '.')
    
    sed -i "s/^LONGPOLL = .*/LONGPOLL = $nuevo_lp   # Ajustado por Victor Perez/" "$PY_SCRIPT"
    systemctl restart bhttp
    echo -e "${GREEN}[✔] Búfer actualizado a ${nuevo_lp}s y servicio reiniciado.${RESET}"
    read -p "Presione ENTER para continuar..."
}

editar_puertos_conexion() {
    if [ ! -f "$SERVICE_FILE" ]; then
        echo -e "${RED}[!] BHTTP no está instalado.${RESET}"
        read -p "Presione ENTER para continuar..."
        return
    fi
    obtener_config_actual
    echo -e "\n${YELLOW}Configuración actual:${RESET}"
    echo -e " - Puerto de Escucha: ${CYAN}$PORT${RESET}"
    echo -e " - Backend Host: ${CYAN}$BHOST${RESET}"
    echo -e " - Backend Puerto: ${CYAN}$BPORT${RESET}"
    
    read -p "Nuevo puerto de escucha [$PORT]: " p_escucha
    p_escucha=${p_escucha:-$PORT}
    
    read -p "Nuevo backend host [$BHOST]: " b_host
    b_host=${b_host:-$BHOST}
    
    read -p "Nuevo backend puerto [$BPORT]: " b_port
    b_port=${b_port:-$BPORT}

    cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Servidor BHTTP Autonomo (Victor Pérez)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=$INSTALL_DIR
ExecStart=/usr/bin/python3 $PY_SCRIPT --port $p_escucha --backend-host $b_host --backend-port $b_port
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

    systemctl daemon-reload
    systemctl restart bhttp
    echo -e "${GREEN}[✔] Configuración actualizada correctamente.${RESET}"
    read -p "Presione ENTER para continuar..."
}

# Auto-instalar el comando global si no existe
if [ ! -f "$MENU_BIN" ]; then
    cp "$0" "$MENU_BIN"
    chmod +x "$MENU_BIN"
fi

while true; do
    mostrar_banner
    echo -e ""
    echo -e "${CYAN}  1. 📥 Instalar BHTTP${RESET}"
    echo -e "${CYAN}  2. 🗑️ Desinstalar BHTTP${RESET}"
    echo -e "${CYAN}  3. 🔌 Cambiar Puerto${RESET}"
    echo -e "${CYAN}  4. ⚡ Editar Búfer${RESET}"
    echo -e "${CYAN}  5. ⚙️ Editar Puerto de Escucha y Conexión${RESET}"
    echo -e "${CYAN}  6. 🔙 Atrás / Actualizar Estado${RESET}"
    echo -e "${CYAN}  7. ❌ Salir${RESET}"
    echo -e ""
    
    if systemctl is-active --quiet bhttp 2>/dev/null; then
        echo -e "  Estado del Servicio: ${GREEN}● ACTIVO (Running)${RESET}"
        obtener_config_actual
        echo -e "  Escuchando en puerto: ${YELLOW}$PORT${RESET} | Backend: ${YELLOW}$BHOST:$BPORT${RESET}"
    else
        echo -e "  Estado del Servicio: ${RED}● INACTIVO / NO INSTALADO${RESET}"
    fi
    echo -e "${MAGENTA}============================================================${RESET}"
    read -p " Seleccione una opción [1-7]: " opcion

    case $opcion in
        1) instalar_bhttp ;;
        2) desinstalar_bhttp ;;
        3) cambiar_puerto ;;
        4) editar_bufer ;;
        5) editar_puertos_conexion ;;
        6) continue ;;
        7) echo -e "\n${GREEN}¡Gracias por usar el Script de Victor Pérez! Saliendo...${RESET}"; exit 0 ;;
        *) echo -e "${RED}[!] Opción inválida. Intente de nuevo.${RESET}"; sleep 1 ;;
    esac
done
