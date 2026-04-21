#!/usr/bin/env python3
"""checkdisk v4 — Linux Disk Health Monitor (High-Contrast Mono Theme)"""

# ─── Bootstrap ────────────────────────────────────────────────────────────────
import subprocess, sys, os

_VENV_DIR    = os.path.join(os.environ.get("XDG_DATA_HOME",
               os.path.expanduser("~/.local/share")), "checkdisk", "venv")
_VENV_PYTHON = os.path.join(_VENV_DIR, "bin", "python3")
_DEPS        = ["rich", "psutil"]

# Supported package managers across common Linux distributions.
# (binary, install_args, package-name-mapping)
_PKG_MANAGERS = [
    ("apt-get", ["install", "-y", "-q"],  {"rich": "python3-rich", "psutil": "python3-psutil"}),
    ("dnf",     ["install", "-y", "-q"],  {"rich": "python3-rich", "psutil": "python3-psutil"}),
    ("yum",     ["install", "-y", "-q"],  {"rich": "python3-rich", "psutil": "python3-psutil"}),
    ("zypper",  ["install", "-y"],        {"rich": "python3-rich", "psutil": "python3-psutil"}),
    ("pacman",  ["-S", "--noconfirm"],    {"rich": "python-rich",  "psutil": "python-psutil"}),
    ("apk",     ["add", "--no-progress"], {"rich": "py3-rich",     "psutil": "py3-psutil"}),
    ("emerge",  ["--quiet"],              {"rich": "dev-python/rich", "psutil": "dev-python/psutil"}),
]

def _deps_present():
    try: import rich, psutil; return True
    except ImportError: return False

def _which(name):
    for p in os.environ.get("PATH", "/usr/bin:/bin:/usr/local/bin").split(os.pathsep):
        cand = os.path.join(p, name)
        if os.path.isfile(cand) and os.access(cand, os.X_OK): return cand
    return None

def _try_system_pkg():
    """Try the native package manager of whatever Linux distro we're on."""
    for binary, args, pkg_map in _PKG_MANAGERS:
        mgr = _which(binary)
        if not mgr: continue
        needed = []
        for d in _DEPS:
            try: __import__(d)
            except ImportError: needed.append(pkg_map[d])
        if not needed: return True
        # Only attempt if we have root or can use sudo
        cmd = [mgr] + args + needed
        if os.geteuid() != 0:
            if not _which("sudo"): continue
            cmd = ["sudo", "-n"] + cmd   # non-interactive sudo
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=180)
            if r.returncode == 0 and _deps_present():
                return True
        except Exception:
            continue
    return False

def _ensure_venv():
    import venv as _v
    if not os.path.isfile(_VENV_PYTHON):
        print(f"[checkdisk] Creating venv at {_VENV_DIR} ...")
        _v.create(_VENV_DIR, with_pip=True, clear=False)
    print("[checkdisk] Installing rich, psutil ...")
    r = subprocess.run([_VENV_PYTHON,"-m","pip","install","--quiet","--upgrade"]+_DEPS,
                       capture_output=True)
    if r.returncode != 0:
        subprocess.run([_VENV_PYTHON,"-m","ensurepip","--upgrade"], capture_output=True)
        subprocess.run([_VENV_PYTHON,"-m","pip","install","--quiet"]+_DEPS, capture_output=True)

def _bootstrap():
    if _deps_present(): return
    running_in_venv = sys.executable.startswith(_VENV_DIR+os.sep)
    if not running_in_venv:
        if _try_system_pkg() and _deps_present(): return
        _ensure_venv()
        print("[checkdisk] Restarting with venv Python ...\n")
        os.execv(_VENV_PYTHON, [_VENV_PYTHON]+sys.argv)
    else:
        _ensure_venv()
        os.execv(_VENV_PYTHON, [_VENV_PYTHON]+sys.argv)

_bootstrap()

# ─── Imports ──────────────────────────────────────────────────────────────────
import json, time, re, shutil, io, threading, termios, tty, random, math
import select as _select
from collections import deque
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, field

import psutil
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from rich.rule import Rule
from rich.align import Align
from rich.columns import Columns

console = Console()

# ─── Data model ───────────────────────────────────────────────────────────────

@dataclass
class DiskInfo:
    device:       str
    name:         str
    model:        str   = "Unknown"
    serial:       str   = "N/A"
    firmware:     str   = "N/A"
    interface:    str   = "SATA"
    size_bytes:   int   = 0
    size_human:   str   = "?"
    health_pct:   int   = -1
    perf_pct:     int   = -1
    temp_c:       Optional[int] = None
    smart_status: str   = "UNKNOWN"
    power_on_hours:       int = 0
    reallocated_sectors:  int = 0
    pending_sectors:      int = 0
    uncorrectable_errors: int = 0
    smart_error_count:    int = 0
    power_cycles:         int = 0
    used_bytes:   int   = 0
    free_bytes:   int   = 0
    total_bytes:  int   = 0
    used_pct:     float = 0.0
    # I/O speed (bytes/s, from /proc/diskstats)
    read_bps:     float = 0.0
    write_bps:    float = 0.0
    # Temperature history (last 20 readings) for sparkline
    temp_history: List[int] = field(default_factory=list)
    smart_attrs:  List[Dict] = field(default_factory=list)
    smart_available: bool   = False
    error_msg:    str  = ""

# ─── Helpers ──────────────────────────────────────────────────────────────────

def run_cmd(cmd: list, timeout: int = 12) -> Tuple[int, str, str]:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout, r.stderr
    except subprocess.TimeoutExpired: return -1, "", "timeout"
    except FileNotFoundError:         return -1, "", f"not found: {cmd[0]}"
    except Exception as e:            return -1, "", str(e)

def human_size(b: int) -> str:
    if b <= 0: return "?"
    for u in ("B","KB","MB","GB","TB","PB"):
        if b < 1024.0: return f"{b:.1f} {u}"
        b /= 1024.0
    return f"{b:.1f} EB"

def human_speed(bps: float) -> str:
    """Always returns exactly 10 chars wide — prevents layout jitter."""
    if bps <= 0:    return "    —     "
    if bps < 1024:  return f"{bps:6.1f} B/s "
    bps /= 1024
    if bps < 1024:  return f"{bps:6.1f} KB/s"
    bps /= 1024
    if bps < 1024:  return f"{bps:6.1f} MB/s"
    bps /= 1024
    return              f"{bps:6.1f} GB/s"

def sparkline(temps: List[int], width: int = 8) -> Text:
    """Mini sparkline of last N temperatures."""
    t = Text()
    if not temps:
        t.append("─" * width, style=T3); return t
    vals = temps[-width:]
    lo, hi = min(vals), max(vals)
    span = max(1, hi - lo)
    bars = " ▁▂▃▄▅▆▇█"
    for v in vals:
        idx = int(((v - lo) / span) * 8)
        c = G if v < 40 else (Y if v < 55 else R)
        t.append(bars[idx], style=c)
    # pad left
    if len(vals) < width:
        padding = "─" * (width - len(vals))
        t2 = Text(padding, style=T3); t2.append_text(t); return t2
    return t

# ─── I/O speed tracking ───────────────────────────────────────────────────────

_io_prev: Dict[str, Tuple[int, int, float]] = {}  # name → (read_sectors, write_sectors, time)

def _read_diskstats() -> Dict[str, Tuple[int, int]]:
    """Read /proc/diskstats → {name: (read_sectors, write_sectors)}"""
    result = {}
    try:
        with open("/proc/diskstats") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 10: continue
                name = parts[2]
                result[name] = (int(parts[5]), int(parts[9]))  # sectors read/written
    except Exception: pass
    return result

def update_io_speeds(disks: List[DiskInfo]):
    """Update read_bps / write_bps on each DiskInfo using /proc/diskstats deltas."""
    global _io_prev
    now     = time.time()
    current = _read_diskstats()
    for d in disks:
        name = d.name
        if name not in current: continue
        rs, ws = current[name]
        if name in _io_prev:
            pr, pw, pt = _io_prev[name]
            dt = now - pt
            if dt > 0.1:
                d.read_bps  = max(0.0, (rs - pr) * 512 / dt)
                d.write_bps = max(0.0, (ws - pw) * 512 / dt)
        _io_prev[name] = (rs, ws, now)

# ─── Disk enumeration ─────────────────────────────────────────────────────────

def list_disk_names() -> List[str]:
    rc, out, _ = run_cmd(["lsblk", "-d", "-n", "-o", "NAME,TYPE"])
    if rc != 0 or not out.strip():
        try:
            return sorted(d for d in os.listdir("/sys/block")
                          if not re.match(r"^(loop|ram|zram)", d))
        except Exception: return []
    names = []
    for line in out.strip().splitlines():
        parts = line.split()
        if not parts: continue
        name, dtype = parts[0], (parts[1] if len(parts)>1 else "")
        if dtype == "disk" or name.startswith("nvme"): names.append(name)
        elif dtype == "" and not re.match(r"^(loop|ram|zram|sr)", name): names.append(name)
    return sorted(names)

def get_lsblk_info(device: str) -> Dict:
    rc, out, _ = run_cmd(["lsblk","-J","-d","-b","-o",
                           "NAME,SIZE,MODEL,SERIAL,TRAN,VENDOR,TYPE,REV",f"/dev/{device}"])
    if rc == 0 and out.strip():
        try:
            devs = json.loads(out).get("blockdevices", [])
            if devs: return devs[0]
        except json.JSONDecodeError: pass
    return {}

def get_disk_usage(device: str) -> Tuple[int, int, int, float]:
    rc, out, _ = run_cmd(["lsblk", "-J", "-o", "NAME,MOUNTPOINT", f"/dev/{device}"])
    if rc != 0 or not out.strip(): return 0, 0, 0, 0.0
    try:
        t_used = t_free = t_total = 0; found = False
        def walk(node):
            nonlocal t_used, t_free, t_total, found
            mp = node.get("mountpoint")
            if mp and mp not in ("", "[SWAP]", None):
                try:
                    u = psutil.disk_usage(mp)
                    t_used+=u.used; t_free+=u.free; t_total+=u.total; found=True
                except Exception: pass
            for child in node.get("children", []): walk(child)
        for d in json.loads(out).get("blockdevices", []): walk(d)
        if found and t_total > 0:
            return t_used, t_free, t_total, (t_used/t_total)*100
    except Exception: pass
    return 0, 0, 0, 0.0

# ─── SMART data ───────────────────────────────────────────────────────────────

def get_smart_json(device: str) -> Dict:
    rc, out, _ = run_cmd(["smartctl", "-j", "-a", f"/dev/{device}"])
    if not out.strip(): return {}
    try: return json.loads(out)
    except json.JSONDecodeError: return {}

def detect_interface(device: str, lbl: Dict, smart: Dict) -> str:
    name = device.lower(); tran = (lbl.get("tran") or "").lower()
    if name.startswith("nvme"): return "NVMe"
    if tran == "usb":           return "USB"
    if tran in ("sata","ata"):  return "SATA"
    if tran == "sas":           return "SAS"
    if name.startswith(("vd","xvd","hv")): return "Virtual"
    if name.startswith("mmcblk"):          return "eMMC"
    if name.startswith("hd"):              return "IDE"
    if "NVM" in smart.get("device",{}).get("protocol","").upper(): return "NVMe"
    return "SATA"

def parse_health_and_perf(smart: Dict) -> Tuple[int,int,int,int,int,int,int,str]:
    if not smart: return -1,-1,0,0,0,0,0,"UNKNOWN"
    ss = smart.get("smart_status",{})
    status = "PASSED" if ss.get("passed") is True else ("FAILED" if ss.get("passed") is False else "UNKNOWN")
    health=100; perf=100; poh=0; realloc=0; pending=0; uncorr=0; power_cycles=0
    for a in smart.get("ata_smart_attributes",{}).get("table",[]):
        aid=a.get("id",0); value=a.get("value",100); thresh=a.get("thresh",0); raw=a.get("raw",{}).get("value",0)
        if thresh>0 and value<=thresh: health=min(health,health-20)
        if aid==5:   realloc=raw; health-=min(40,raw*2) if raw>0 else 0
        elif aid==9:  poh=raw; health-=(25 if raw>50000 else 15 if raw>35000 else 8 if raw>20000 else 3 if raw>10000 else 0)
        elif aid==10: health-=min(10,raw*2) if raw>0 else 0
        elif aid==12: power_cycles=raw
        elif aid==187: uncorr=raw; health-=min(35,raw*5) if raw>0 else 0
        elif aid==196: health-=min(10,raw*2) if raw>0 else 0
        elif aid==197: pending=raw; health-=min(30,raw*3) if raw>0 else 0
        elif aid==198:
            if raw>uncorr: uncorr=raw
            health-=min(40,raw*4) if raw>0 else 0
        elif aid==199: health-=5 if raw>200 else 0
        elif aid==2:  perf-=max(0,(100-value)//6) if value<100 else 0
        elif aid==7:  perf-=25 if thresh>0 and value<thresh else 0
        elif aid==193: perf-=10 if raw>600000 else 0
    nvme = smart.get("nvme_smart_health_information_log",{})
    if nvme:
        pct_used=nvme.get("percentage_used",0); health=max(0,100-pct_used)
        poh=nvme.get("power_on_hours",poh); power_cycles=nvme.get("power_cycles",power_cycles)
        media_errors=nvme.get("media_errors",0); uncorr=media_errors
        if media_errors>0: health-=min(30,media_errors*5)
        avail=nvme.get("available_spare",100); thresh=nvme.get("available_spare_threshold",10)
        if avail<=thresh: health=min(health,20)
        perf=max(0,min(100,avail))
        if nvme.get("critical_warning",0): health-=20
    if status=="FAILED": health=min(health,5)
    return (max(0,min(100,health)),max(0,min(100,perf)),poh,realloc,pending,uncorr,power_cycles,status)

def get_temperature(smart: Dict) -> Optional[int]:
    t_obj = smart.get("temperature",{})
    if isinstance(t_obj,dict) and t_obj.get("current") is not None: return int(t_obj["current"])
    nvme = smart.get("nvme_smart_health_information_log",{})
    if nvme:
        sensors = nvme.get("temperature_sensors",[])
        if sensors: return int(sensors[0])
        t = nvme.get("temperature")
        if t is not None: return int(t)
    for a in smart.get("ata_smart_attributes",{}).get("table",[]):
        if a.get("id") in (190,194):
            t = a.get("raw",{}).get("value",0) & 0xFF
            if 5<=t<=85: return t
    return None

def get_firmware(smart: Dict) -> str:   return smart.get("firmware_version","N/A") or "N/A"
def get_poh(smart: Dict) -> int:        return smart.get("power_on_time",{}).get("hours",0) or 0

def get_smart_error_count(smart: Dict) -> int:
    e = smart.get("ata_smart_error_log",{})
    if isinstance(e,dict):
        return max(e.get("extended",{}).get("count",0) or 0,
                   e.get("summary",{}).get("count",0) or 0)
    return 0

def build_smart_attrs_table(smart: Dict) -> List[Dict]:
    attrs = []
    for a in smart.get("ata_smart_attributes",{}).get("table",[]):
        thresh=a.get("thresh",0); value=a.get("value",100)
        failed=a.get("when_failed","") not in ("","-")
        attrs.append({"id":a.get("id","?"),
                      "name":a.get("name","Unknown").replace("_"," "),
                      "value":str(value),"worst":str(a.get("worst","?")),"thresh":str(thresh),
                      "raw":str(a.get("raw",{}).get("string",a.get("raw",{}).get("value","?"))),
                      "failed":failed or (thresh>0 and value<=thresh)})
    nvme = smart.get("nvme_smart_health_information_log",{})
    if nvme and not attrs:
        for key,label in [
            ("critical_warning","Critical Warning"),("temperature","Temperature (°C)"),
            ("available_spare","Available Spare (%)"),("available_spare_threshold","Spare Threshold (%)"),
            ("percentage_used","Life Used (%)"),("data_units_read","Data Read"),
            ("data_units_written","Data Written"),("power_cycles","Power Cycles"),
            ("power_on_hours","Power On Hours"),("unsafe_shutdowns","Unsafe Shutdowns"),
            ("media_errors","Media Errors"),("num_err_log_entries","Error Log Entries"),
        ]:
            val=nvme.get(key)
            if val is None: continue
            attrs.append({"id":"—","name":label,"value":str(val),"worst":"—","thresh":"—",
                          "raw":str(val),"failed":key in ("critical_warning","media_errors") and val not in (0,"0")})
    return attrs

# ─── Full fetch ────────────────────────────────────────────────────────────────

def fetch_disk(device: str, prev: Optional[DiskInfo] = None) -> DiskInfo:
    info = DiskInfo(device=f"/dev/{device}", name=device)
    lbl = get_lsblk_info(device)
    info.model  = ((lbl.get("model") or lbl.get("vendor") or "Unknown").strip())[:40]
    info.serial = (lbl.get("serial") or "N/A").strip()
    try: info.size_bytes = int(lbl.get("size") or 0)
    except (ValueError,TypeError): info.size_bytes = 0
    info.size_human = human_size(info.size_bytes)

    smart = get_smart_json(device)
    info.smart_available = bool(smart)
    info.interface = detect_interface(device, lbl, smart)
    if smart:
        (info.health_pct, info.perf_pct, info.power_on_hours,
         info.reallocated_sectors, info.pending_sectors,
         info.uncorrectable_errors, info.power_cycles,
         info.smart_status) = parse_health_and_perf(smart)
        info.temp_c            = get_temperature(smart)
        info.firmware          = get_firmware(smart)
        info.smart_error_count = get_smart_error_count(smart)
        info.smart_attrs       = build_smart_attrs_table(smart)
        if info.power_on_hours == 0: info.power_on_hours = get_poh(smart)
    else:
        info.smart_status = "NO DATA"

    info.used_bytes, info.free_bytes, info.total_bytes, info.used_pct = get_disk_usage(device)

    # Carry over temperature history and I/O speeds from previous fetch
    if prev is not None:
        info.temp_history = list(prev.temp_history)
        info.read_bps     = prev.read_bps
        info.write_bps    = prev.write_bps
    if info.temp_c is not None:
        info.temp_history.append(info.temp_c)
        if len(info.temp_history) > 20: info.temp_history = info.temp_history[-20:]

    return info


# ─── Nord theme ───────────────────────────────────────────────────────────────
#
#  Palette: Nord — https://www.nordtheme.com
#  Polar Night:  #2e3440  #3b4252  #434c5e  #4c566a
#  Snow Storm:   #d8dee9  #e5e9f0  #eceff4
#  Frost:        #8fbcbb  #88c0d0  #81a1c1  #5e81ac
#  Aurora:       #bf616a  #d08770  #ebcb8b  #a3be8c  #b48ead
#
# ──────────────────────────────────────────────────────────────────────────────

# Box & chrome
BOX_BORDER  = "#4c566a"            # Polar Night 4  — panel borders
BOX_TITLE   = "bold #88c0d0"       # Frost 2        — default panel titles
BOX_TITLE2  = "bold #b48ead"       # Aurora purple  — secondary accents
BOX_DIV     = "#3b4252"            # Polar Night 2  — dividers

# Selected row only — NO alternating stripe, to keep the selection unambiguous
SEL_BG      = "on #3b4c63"         # cool blue-grey tint (distinct from any grey)
SEL_FG      = "bold #eceff4"       # Snow Storm 3
ALT_BG      = ""                   # ← alternating stripe removed on purpose

# Text hierarchy
FG0 = "bold #eceff4"   # Snow Storm 3 — primary emphasis / selected
FG1 = "#e5e9f0"         # Snow Storm 2 — normal body
FG2 = "#d8dee9"         # Snow Storm 1 — secondary
FG3 = "#9aa3b0"         # mid-tone     — dim / muted
FG4 = "#4c566a"         # Polar Night 4 — near-invisible decorative

# Status — semantic colour only
CG  = "#a3be8c"         # green  — HEALTHY
CY  = "#ebcb8b"         # yellow — WARN
CR  = "#bf616a"         # red    — CRITICAL
CT  = "#4c566a"         # dim    — no data

# Interface type accent colours
IF_C = {
    "NVMe":    "bold #88c0d0",
    "SATA":    "#e5e9f0",
    "USB":     "#8fbcbb",
    "SAS":     "#81a1c1",
    "Virtual": "#ebcb8b",
    "eMMC":    "#a3be8c",
    "IDE":     "#4c566a",
}

# 8-level sub-character blocks (smooth bars)
_BAR = " ▏▎▍▌▋▊▉█"

# ─── Neutral symbol set (no emoji) ────────────────────────────────────────────
# Using non-emoji Unicode glyphs that render the same in every terminal font.
SYM_OK    = "●"   # filled circle            — healthy / good
SYM_WARN  = "!"   # exclamation               — warning
SYM_CRIT  = "x"   # lowercase x               — critical
SYM_SEL   = "▶"   # right-pointing triangle   — selected
SYM_UP    = "▲"
SYM_DOWN  = "▼"
SYM_CHECK = "v"   # lowercase v as checkmark  — completed action
SYM_BAR_V = "▌"   # solid vertical bar for left accent


HELP_TEXT = f"""
[{BOX_BORDER}]╭─[/{BOX_BORDER}] [{BOX_TITLE}]CHECKDISK  v4[/{BOX_TITLE}] [{FG2}]─  Linux Disk Health Monitor  ──────────────────────────[/{FG2}][{BOX_BORDER}]╮[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]                                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]  [{FG0}]COMMANDS[/{FG0}]                                                                [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG1}]checkdisk all[/{FG1}]                 Interactive dashboard                [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG1}]checkdisk /dev/sda[/{FG1}]            Detail view for one disk            [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG1}]checkdisk clean /dev/sda[/{FG1}]      Fast wipe  (partition tables only)  [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG1}]checkdisk clean-all /dev/sda[/{FG1}]  Secure erase  (zeros entire disk)   [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG1}]checkdisk --help[/{FG1}]              This screen                         [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]                                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]  [{FG0}]DASHBOARD KEYS[/{FG0}]                                                         [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG0}]↑ ↓  /  j k[/{FG0}]    Navigate rows                                     [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG0}]C[/{FG0}]                 Open clean / wipe dialog                      [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG0}]I[/{FG0}]                 Run I/O & IOPS benchmark                      [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG0}]R[/{FG0}]                 Force data refresh                            [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [{FG0}]Q  /  Ctrl-C[/{FG0}]      Quit                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]                                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]  [{FG0}]STATUS LEGEND[/{FG0}]                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [bold {CG}]{SYM_OK}  GOOD   80–100%[/bold {CG}]  drive is healthy                               [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [bold {CY}]{SYM_WARN}  WARN   50–79%[/bold {CY}]  monitor closely                               [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [bold {CR}]{SYM_CRIT}  CRIT    0–49%[/bold {CR}]  replace soon                                  [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]                                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]  [{FG0}]REQUIREMENTS[/{FG0}]                                                           [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    Install smartmontools via your distro package manager                  [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    Install nvme-cli for NVMe detail (optional)                            [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    [bold {CY}]Run with sudo for full SMART access[/bold {CY}]                               [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]                                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]  [{FG0}]INSTALL[/{FG0}]                                                                [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    sudo cp checkdisk.py /usr/local/bin/checkdisk                       [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]    sudo chmod +x /usr/local/bin/checkdisk                              [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]│[/{BOX_BORDER}]                                                                          [{BOX_BORDER}]│[/{BOX_BORDER}]
[{BOX_BORDER}]╰──────────────────────────────────────────────────────────────────────────╯[/{BOX_BORDER}]
"""


# ─── Bar & cell renderers ──────────────────────────────────────────────────────

# ─── Colour-interpolation helpers (for smooth multi-stop gradients) ───────────
def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
    h = h.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)

def _rgb_to_hex(r: int, g: int, b: int) -> str:
    clamp = lambda x: max(0, min(255, int(x)))
    return f"#{clamp(r):02x}{clamp(g):02x}{clamp(b):02x}"

def _lerp(a: str, b: str, t: float) -> str:
    t = max(0.0, min(1.0, t))
    r1, g1, b1 = _hex_to_rgb(a)
    r2, g2, b2 = _hex_to_rgb(b)
    return _rgb_to_hex(r1+(r2-r1)*t, g1+(g2-g1)*t, b1+(b2-b1)*t)

# Gradient stops — Aurora red → yellow → green
_GRAD_HIGH_GOOD = [(0.0, "#bf616a"), (0.5, "#ebcb8b"), (1.0, "#a3be8c")]
_GRAD_LOW_GOOD  = [(0.0, "#a3be8c"), (0.5, "#ebcb8b"), (1.0, "#bf616a")]

def _grad_at(pos: float, invert: bool) -> str:
    stops = _GRAD_HIGH_GOOD if invert else _GRAD_LOW_GOOD
    pos   = max(0.0, min(1.0, pos))
    for i in range(len(stops) - 1):
        s0, c0 = stops[i]
        s1, c1 = stops[i + 1]
        if pos <= s1:
            local = (pos - s0) / (s1 - s0) if s1 > s0 else 0.0
            return _lerp(c0, c1, local)
    return stops[-1][1]


def _gradient_bar(pct: float, width: int, invert: bool = False) -> Text:
    """Status bar with red/yellow/green gradient. Use for good/bad grading."""
    t = Text()
    if pct < 0:
        t.append("─" * width, style=FG4)
        return t
    pct     = max(0.0, min(100.0, pct))
    eighths = int(round((pct / 100.0) * width * 8))
    full    = eighths // 8
    partial = eighths % 8
    empty   = width - full - (1 if partial else 0)

    for i in range(full):
        pos = (i + 1) / width
        t.append("█", style=f"bold {_grad_at(pos, invert)}")
    if partial and full < width:
        pos = (full + 1) / width
        t.append(_BAR[partial], style=f"bold {_grad_at(pos, invert)}")
    t.append("░" * max(0, empty), style=FG4)
    return t


def _progress_bar(pct: float, width: int, t_: float = 0.0) -> Text:
    """Progress bar — solid blue fill with animated shimmer. Use for 0%→100%."""
    HEAD      = "#8fbcbb"   # Frost 0  — leading edge
    MID       = "#88c0d0"   # Frost 1  — main fill
    TAIL      = "#5e81ac"   # Frost 3  — tail
    EMPTY_COL = "#3b4252"   # Polar Night 1 — unfilled track

    t = Text()
    if pct < 0:
        t.append("─" * width, style=FG4)
        return t
    pct     = max(0.0, min(100.0, pct))
    eighths = int(round((pct / 100.0) * width * 8))
    full    = eighths // 8
    partial = eighths % 8
    empty   = width - full - (1 if partial else 0)

    shimmer_pos = int((t_ * 6) % max(1, full + 1)) if full > 0 else -1

    # Filled cells: blend TAIL → MID → HEAD based on position within filled
    for i in range(full):
        if full > 1:
            local = i / (full - 1)
        else:
            local = 1.0
        # First half: TAIL → MID, second half: MID → HEAD
        if local < 0.5:
            col = _lerp(TAIL, MID, local * 2)
        else:
            col = _lerp(MID, HEAD, (local - 0.5) * 2)
        # Shimmer cell = extra-bright highlight that walks along
        if i == shimmer_pos:
            t.append("█", style=f"bold {HEAD}")
        else:
            t.append("█", style=col)

    # Partial (sub-cell) at the leading edge — always the brightest colour
    if partial and full < width:
        t.append(_BAR[partial], style=f"bold {HEAD}")

    # Unfilled track — subtle, not jarring
    t.append("░" * max(0, empty), style=EMPTY_COL)
    return t


def _hc(p: int) -> str:  return CG if p>=80 else (CY if p>=50 else (CT if p<0 else CR))
def _uc(p: float) -> str: return CG if p<70 else (CY if p<90 else CR)
def _tc(t: Optional[int]) -> str:
    if t is None: return CT
    return CG if t<40 else (CY if t<55 else CR)

def _free_cell(info: "DiskInfo") -> Text:
    """Free-space cell — right-aligned, green when plenty, yellow when low, red when critical."""
    t = Text()
    if info.total_bytes == 0:
        t.append("     —    ", style=FG3)
        return t
    free_pct = (info.free_bytes / info.total_bytes) * 100 if info.total_bytes else 0
    # Invert: free-space warning thresholds are the mirror of used-space thresholds
    c = CG if free_pct > 30 else (CY if free_pct > 10 else CR)
    t.append(f"{human_size(info.free_bytes):>10}", style=f"bold {c}")
    return t

def _usage_cell(info: "DiskInfo") -> Text:
    if info.total_bytes == 0:
        return Text("  —", style=FG3)
    c = _uc(info.used_pct)
    t = Text()
    t.append_text(_gradient_bar(info.used_pct, 8, invert=False))
    t.append(f" {info.used_pct:4.0f}%", style=f"bold {c}")
    return t

def _status_cell(info: "DiskInfo") -> Text:
    t = Text()
    if info.smart_status=="FAILED" or (0<=info.health_pct<50):
        t.append(f"{SYM_CRIT} CRIT", style=f"bold {CR}")
    elif info.health_pct<0 and info.smart_status in ("UNKNOWN","NO DATA"):
        t.append("? N/A ", style=FG3)
    elif info.health_pct<80:
        t.append(f"{SYM_WARN} WARN", style=f"bold {CY}")
    else:
        t.append(f"{SYM_OK} GOOD", style=f"bold {CG}")
    return t

def _iface_cell(iface: str) -> Text:
    return Text(iface, style=IF_C.get(iface, FG2))

def _action_cell(dev: str, sel: bool, cst: Dict, t_: float) -> Text:
    st = cst.get(dev, "")
    tx = Text()
    if st == "cleaning":
        spin = "⣾⣽⣻⢿⡿⣟⣯⣷"[int(t_*10)%8]
        tx.append(f"{spin} wiping…", style=f"bold {CY}")
    elif st == "done":
        tx.append(f"{SYM_CHECK} cleaned", style=f"bold {CG}")
    elif st == "failed":
        tx.append(f"{SYM_CRIT} error  ", style=f"bold {CR}")
    elif st == "iops":
        spin = "⣾⣽⣻⢿⡿⣟⣯⣷"[int(t_*10)%8]
        tx.append(f"{spin} testing…", style=f"bold {BOX_TITLE2[5:]}")
    elif sel:
        tx.append("[", style=FG3)
        tx.append("C", style=FG0)
        tx.append("] wipe", style=f"bold {CR}")
    else:
        tx.append("[ ] wipe", style=FG3)
    return tx

def score_label(pct: int) -> str:
    if pct<0:   return "NO DATA"
    if pct>=95: return "EXCELLENT"
    if pct>=80: return "GOOD"
    if pct>=60: return "FAIR"
    if pct>=40: return "POOR"
    return "CRITICAL"

def temp_label(t: Optional[int]) -> str:
    if t is None: return "N/A"
    return "COOL" if t<35 else ("NORMAL" if t<45 else ("WARM" if t<55 else "HOT"))

def _poh_fmt(h: int) -> str:
    if not h: return "—"
    if h>=8760: return f"{h//8760}y{(h%8760)//730}m"
    if h>=730:  return f"{h//730}mo"
    return f"{h//24}d"


# ─── btop-style Panel builder ──────────────────────────────────────────────────

def btop_panel(content, title: str = "", subtitle: str = "",
               border: str = BOX_BORDER, title_style: Optional[str] = None) -> Panel:
    """Render a Nord-styled panel. `title_style` overrides default Frost blue."""
    ts = title_style or BOX_TITLE
    t  = f"[{ts}] {title} [/{ts}]" if title else ""
    s  = f"[{FG3}] {subtitle} [/{FG3}]" if subtitle else ""
    return Panel(content, title=t, subtitle=s,
                 border_style=border, box=box.ROUNDED, padding=(0, 1),
                 expand=True)

def _section_rule(label: str = "") -> Rule:
    """Nord-styled divider with optional label."""
    if label:
        return Rule(Text(f" {label} ", style=FG3), style=BOX_DIV)
    return Rule(style=BOX_DIV)


# ─── TUI state ─────────────────────────────────────────────────────────────────

@dataclass
class _State:
    disks:        List[DiskInfo]     = field(default_factory=list)
    selected:     int                = 0
    modal_disk:   Optional[DiskInfo] = None
    modal_btn:    int                = 0
    clean_status: Dict[str, str]     = field(default_factory=dict)
    quit:         bool               = False
    lock:         threading.Lock     = field(default_factory=threading.Lock)
    refresh_ev:   threading.Event    = field(default_factory=threading.Event)
    _dirty:       bool               = True
    # IOPS/I-O benchmark state
    iops_running:     bool               = False
    iops_disk:        Optional[DiskInfo] = None
    iops_phase:       str                = ""       # internal phase key
    iops_phase_label: str                = ""       # human-readable phase label
    iops_phase_num:   int                = 0        # 1..N
    iops_phase_total: int                = 0        # N
    iops_pct:         int                = 0        # phase progress 0..100
    iops_overall:     int                = 0        # overall 0..100
    iops_info:        str                = ""       # status line
    iops_live:        str                = ""       # live measurement line
    iops_result:      Optional[Dict]     = None     # populated when test finishes
    iops_cancel:      bool               = False    # user-requested cancel


# ─── ANSI helpers ─────────────────────────────────────────────────────────────

_ALT_ON   = "\033[?1049h"
_ALT_OFF  = "\033[?1049l"
_CUR_ON   = "\033[?25h"
_CUR_OFF  = "\033[?25l"
_HOME     = "\033[H"
_CLR_END  = "\033[J"
_SYNC_ON  = "\033[?2026h"      # begin synchronized update (anti-flicker)
_SYNC_OFF = "\033[?2026l"      # end synchronized update

def _term_size() -> Tuple[int, int]:
    try: sz = shutil.get_terminal_size(fallback=(120, 35)); return sz.columns, sz.lines
    except Exception: return 120, 35

# Terminal is set ONCE to cbreak+noecho at the start of cmd_all() and restored
# at exit.  This prevents unhandled keypresses (PgUp, F-keys, mouse events etc.)
# from echoing stray characters onto the screen.
_orig_tty_attr = None

def _tty_setup():
    """Put stdin into cbreak + noecho for the whole TUI session."""
    global _orig_tty_attr
    if not sys.stdin.isatty(): return
    fd = sys.stdin.fileno()
    _orig_tty_attr = termios.tcgetattr(fd)
    new = termios.tcgetattr(fd)
    # Disable ECHO (don't echo typed chars) and ICANON (read single chars, no buffering).
    # ONLCR stays on so '\n' still translates to '\r\n' when we write the frame.
    new[3] = new[3] & ~(termios.ECHO | termios.ICANON)
    # cc[VMIN]=0, cc[VTIME]=0 → read() returns immediately with whatever is available
    new[6][termios.VMIN]  = 0
    new[6][termios.VTIME] = 0
    termios.tcsetattr(fd, termios.TCSANOW, new)

def _tty_restore():
    """Restore the original terminal mode."""
    global _orig_tty_attr
    if _orig_tty_attr is None or not sys.stdin.isatty(): return
    try:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, _orig_tty_attr)
    except Exception:
        pass
    _orig_tty_attr = None

def _read_key() -> Optional[str]:
    """Non-blocking key read.  Fully drains any escape sequence so stray
    bytes never leak to the terminal or to the next read.  Returns None if
    no key is available within 50 ms."""
    if not sys.stdin.isatty(): return None
    fd = sys.stdin.fileno()
    try:
        if not _select.select([sys.stdin], [], [], 0.05)[0]: return None
        ch = os.read(fd, 1).decode("utf-8", errors="replace")
        if not ch: return None
        # Drain the rest of an escape sequence non-blockingly
        if ch == "\033":
            # Read up to 8 more bytes with a tiny timeout; stop when buffer is empty
            for _ in range(8):
                if not _select.select([sys.stdin], [], [], 0.008)[0]: break
                try:
                    extra = os.read(fd, 1).decode("utf-8", errors="replace")
                    if not extra: break
                    ch += extra
                except Exception: break
        return ch
    except Exception:
        return None


# ─── Header panel ──────────────────────────────────────────────────────────────

def _header_panel(state: "_State", ts: str) -> Panel:
    with state.lock:
        disks = list(state.disks); sel = state.selected

    n     = len(disks)
    n_ok  = sum(1 for d in disks if d.health_pct>=80 or d.smart_status=="PASSED")
    n_bad = sum(1 for d in disks if d.smart_status=="FAILED" or (0<=d.health_pct<50))
    n_warn= n - n_ok - n_bad

    # Branding row
    lft = Text()
    lft.append("  ")
    # Letter-by-letter gradient on CHECKDISK: Frost teal → Aurora purple
    _brand      = "CHECKDISK"
    _brand_from = "#8fbcbb"   # Frost 1 teal
    _brand_to   = "#b48ead"   # Aurora purple
    for i, ch in enumerate(_brand):
        col = _lerp(_brand_from, _brand_to, i / max(1, len(_brand) - 1))
        lft.append(ch, style=f"bold {col}")
    lft.append("  │ ", style=FG4)
    lft.append("Linux Disk Health Monitor", style=FG2)
    lft.append("  v4", style=FG4)

    rgt = Text(justify="right")
    rgt.append(ts, style=FG3)

    brand = Table(show_header=False, box=None, padding=0, expand=True)
    brand.add_column("l", ratio=4); brand.add_column("r", ratio=2, justify="right")
    brand.add_row(lft, rgt)

    # Status summary row
    ok_s  = f"bold {CG}" if n_ok   else FG4
    wa_s  = f"bold {CY}" if n_warn else FG4
    ba_s  = f"bold {CR}" if n_bad  else FG4

    stat = Text()
    stat.append("  ")
    stat.append(f"{n}", style=FG0)
    stat.append(f" disk{'s' if n!=1 else ''}", style=FG2)
    stat.append("   ┃ ", style=FG4)
    stat.append(f"{SYM_OK} ",    style=ok_s); stat.append(f"{n_ok} healthy",  style=ok_s)
    stat.append("   ┃ ", style=FG4)
    stat.append(f"{SYM_WARN} ",  style=wa_s); stat.append(f"{n_warn} warning", style=wa_s)
    stat.append("   ┃ ", style=FG4)
    stat.append(f"{SYM_CRIT} ",  style=ba_s); stat.append(f"{n_bad} critical", style=ba_s)

    # Selected disk quick-info (right-aligned)
    sel_txt = Text(justify="right")
    if disks and 0<=sel<len(disks):
        d = disks[sel]
        sel_txt.append(f"{SYM_SEL} ", style="bold #b48ead")
        sel_txt.append(d.device, style=FG0)
        if d.model != "Unknown":
            sel_txt.append(f"  {d.model[:28]}", style=FG2)
        if d.size_human != "?":
            sel_txt.append(f"  {d.size_human}", style=FG3)

    stat_row = Table(show_header=False, box=None, padding=0, expand=True)
    stat_row.add_column("l", ratio=5); stat_row.add_column("r", ratio=4, justify="right")
    stat_row.add_row(stat, sel_txt)

    body = Table(show_header=False, box=None, padding=(0,0), expand=True)
    body.add_column("c")
    body.add_row(brand)
    body.add_row(Rule(style=BOX_DIV))
    body.add_row(stat_row)

    return btop_panel(body, "checkdisk")


# ─── Disk table panel ──────────────────────────────────────────────────────────

def _disk_panel(state: "_State", t_: float) -> Panel:
    with state.lock:
        disks = list(state.disks); sel = state.selected; cst = dict(state.clean_status)

    tbl = Table(
        show_header  = True,
        header_style = "bold #88c0d0",        # Nord Frost 2
        box          = box.SIMPLE_HEAVY,
        border_style = BOX_DIV,
        expand       = True,
        padding      = (0, 1),
        show_edge    = True,
        leading      = 0,
    )
    # New order: # DEVICE MODEL SIZE FREE USAGE HEALTH PERF STATUS IF AGE ACTION
    tbl.add_column(" # ",     width=4,         justify="right",  no_wrap=True)
    tbl.add_column("DEVICE",  min_width=13,                       no_wrap=True)
    tbl.add_column("MODEL",   min_width=16, max_width=22, style=FG1, no_wrap=True)
    tbl.add_column("SIZE",    justify="right", min_width=8,       no_wrap=True, style=FG2)
    tbl.add_column("FREE",    justify="right", min_width=10,      no_wrap=True)
    tbl.add_column("USAGE",   min_width=14,                       no_wrap=True)
    tbl.add_column("HEALTH",  min_width=14,                       no_wrap=True)
    tbl.add_column("PERF",    min_width=14,                       no_wrap=True)
    tbl.add_column("STATUS",  min_width=7,                        no_wrap=True)
    tbl.add_column("IF",      justify="center", min_width=6,       no_wrap=True)
    tbl.add_column("AGE",     justify="right",  min_width=5,       no_wrap=True, style=FG2)
    tbl.add_column("ACTION",  min_width=11,                        no_wrap=True)

    if not disks:
        tbl.add_row(*[Text("—", style=FG3)] * 12)
    else:
        for i, d in enumerate(disks):
            is_sel = (i == sel)
            # NO alternating stripe: only the selected row gets a background,
            # so there's never ambiguity between "alt row" and "selected row".
            row_bg = SEL_BG if is_sel else ""

            # # cell — arrow + number
            num = Text()
            if is_sel:
                num.append(f"{SYM_SEL}{i+1:2}", style=SEL_FG)
            else:
                num.append(f"  {i+1}", style=FG3)

            # Device name
            dev = Text()
            dev.append(d.device, style=(SEL_FG if is_sel else FG1))

            # Health bar
            hc = _hc(d.health_pct)
            ht = Text()
            ht.append_text(_gradient_bar(d.health_pct, 8, invert=True))
            ht.append(f" {d.health_pct:>3}%" if d.health_pct>=0 else "  N/A",
                      style=f"bold {hc}")

            # Perf bar
            pc = _hc(d.perf_pct)
            pt = Text()
            pt.append_text(_gradient_bar(d.perf_pct, 8, invert=True))
            pt.append(f" {d.perf_pct:>3}%" if d.perf_pct>=0 else "  N/A",
                      style=f"bold {pc}")

            tbl.add_row(
                num,
                dev,
                Text(d.model[:22], style=(SEL_FG if is_sel else FG1)),
                Text(d.size_human),
                _free_cell(d),
                _usage_cell(d),
                ht,
                pt,
                _status_cell(d),
                _iface_cell(d.interface),
                Text(_poh_fmt(d.power_on_hours)),
                _action_cell(d.device, is_sel, cst, t_),
                style=row_bg,
            )

    n_disks = len(disks)
    sub = f"{n_disks} device{'s' if n_disks!=1 else ''} detected"
    return btop_panel(tbl, "storage devices", sub)


# ─── Confirm modal ─────────────────────────────────────────────────────────────

def _modal_panel(disk: DiskInfo, btn: int) -> Panel:
    no_s  = f"bold {CR} on color(88)" if btn==0 else f"{FG2} on color(233)"
    yes_s = f"bold {CG} on color(22)" if btn==1 else f"{FG2} on color(233)"

    info = Table(show_header=False, box=None, padding=(0,3), expand=True)
    info.add_column("k", style=FG2, min_width=10, no_wrap=True)
    info.add_column("v")
    info.add_row(Text("Device",   style=FG2), Text(disk.device,    style=f"bold {CR}"))
    info.add_row(Text("Model",    style=FG2), Text(disk.model,     style=FG1))
    info.add_row(Text("Serial",   style=FG2), Text(disk.serial,    style=FG2))
    info.add_row(Text("Capacity", style=FG2), Text(disk.size_human,style=f"bold {CY}"))
    if disk.total_bytes>0:
        info.add_row(Text("Data", style=FG2),
                     Text(f"{human_size(disk.used_bytes)} currently stored", style=FG1))

    warn = Text(justify="center")
    warn.append(f"\n  {SYM_WARN}  ALL DATA WILL BE PERMANENTLY DESTROYED  {SYM_WARN}  \n",
                style=f"bold {CR}")

    btns = Table(show_header=False, box=None, padding=(0,3), expand=True)
    btns.add_column("n", justify="center", ratio=1)
    btns.add_column("g", width=4)
    btns.add_column("y", justify="center", ratio=1)
    btns.add_row(
        Text(f"  {SYM_CRIT}  NO, CANCEL  ",  style=no_s,  justify="center"),
        Text(""),
        Text(f"  {SYM_CHECK}  YES, WIPE  ",  style=yes_s, justify="center"),
    )
    hint = Text("\n  ←→ switch    Enter confirm    Esc / N cancel  \n",
                style=FG3, justify="center")

    body = Table(show_header=False, box=None, padding=(0,1), expand=True)
    body.add_column("c", justify="center")
    body.add_row(info); body.add_row(Rule(style=FG4))
    body.add_row(warn); body.add_row(btns); body.add_row(hint)

    return Panel(
        Align(body, align="center"),
        title    = f"[bold {CR}]  {SYM_WARN}  CONFIRM WIPE: {disk.device}  {SYM_WARN}  [/bold {CR}]",
        subtitle = f"[{FG3}] fast clean — destroys partition tables & signatures [{FG3}]",
        border_style = CR,
        box          = box.ROUNDED,
        padding      = (0, 2),
    )


# ─── I/O benchmark (fio-based) ────────────────────────────────────────────────
#
# Three tests run back-to-back in one fio invocation:
#   1. Sequential read  — 1M blocks, QD=32, 30s  → peak throughput (MB/s)
#   2. Random 4K IOPS   — 4K blocks, QD=32, 60s  → vendor-headline IOPS
#   3. Latency          — 4K blocks, QD=1,  30s  → p50/p95/p99/p999 latency
#
# All phases use direct I/O (O_DIRECT when supported) and a 5-second ramp
# that's excluded from measurements. Methodology follows SNIA SSS-PTS.
# Read-only; safe to run on mounted disks (tests a temp file on the
# largest writable mount).

# Reference performance ranges per (interface, media) class.
# Each tuple: (seq_min, seq_typ, seq_max, iops_min, iops_typ, iops_max,
#              p99_max_us, p99_typ_us, label)
# Sourced from vendor datasheets and SNIA SSS-PTS reference tests.
_IO_REFERENCE = {
    ("SATA",    "HDD"):   (80,   140,   200,       75,     110,       200,
                          20000,   10000,   "HDD · SATA"),
    ("SAS",     "HDD"):   (140,  200,   260,      100,     175,       300,
                          15000,    8000,   "HDD · SAS"),
    ("USB",     "HDD"):   (60,   100,   140,       80,     140,       220,
                          30000,   15000,   "HDD · USB"),
    ("SATA",    "SSD"):   (450,  520,   560,    50000,   80000,    100000,
                           5000,     500,   "SSD · SATA"),
    ("SAS",     "SSD"):   (700,  950,  1200,    80000,  150000,    200000,
                           3000,     250,   "SSD · SAS (12G)"),
    ("USB",     "SSD"):   (300,  700,  1050,    30000,   55000,     80000,
                           8000,    1000,   "SSD · USB"),
    ("NVMe",    "SSD"):   (2000, 3500, 7000,   300000,  500000,   1000000,
                           1000,     100,   "SSD · NVMe"),
    ("eMMC",    "SSD"):   (150,  260,   400,     5000,   12000,     25000,
                          10000,    2000,   "SSD · eMMC"),
    ("USB",   "FLASH"):   (20,   120,   300,      500,    5000,     15000,
                          50000,   20000,   "USB Flash"),
    ("Virtual", "SSD"):   (200,  800,  2500,    20000,  100000,    400000,
                          10000,    1000,   "Virtual Disk"),
    ("SATA", "UNKNOWN"):  (80,   300,   560,      200,   10000,    100000,
                          15000,    1000,   "SATA"),
    ("USB",  "UNKNOWN"):  (30,   150,  1000,      500,   10000,     50000,
                          20000,    2000,   "USB"),
    ("NVMe", "UNKNOWN"):  (1500, 3500, 7000,   200000,  500000,   1000000,
                           1500,     150,   "NVMe"),
}

def _media_type(info: DiskInfo) -> str:
    """HDD vs SSD heuristic.  Uses SMART rotation_rate when available."""
    try:
        smart = get_smart_json(info.name)
    except Exception:
        smart = {}
    rr = smart.get("rotation_rate", 0) if isinstance(smart, dict) else 0
    if isinstance(rr, int) and rr > 0: return "HDD"
    if info.interface == "USB":
        model = (info.model or "").lower()
        if any(k in model for k in ("flash", "stick", "sandisk cruzer", "cruzer")):
            return "FLASH"
        return "SSD"
    return "SSD" if info.interface in ("NVMe", "eMMC") else "UNKNOWN"

def _io_reference(info: DiskInfo):
    key = (info.interface, _media_type(info))
    if key in _IO_REFERENCE: return _IO_REFERENCE[key]
    for k in _IO_REFERENCE:
        if k[0] == info.interface: return _IO_REFERENCE[k]
    return (100, 500, 2000, 1000, 50000, 500000, 15000, 1000, "Generic")

def _grade_score(value: float, lo: float, typ: float, hi: float) -> Tuple[float, str, str]:
    """(0-1 position, label, color) for a measured value vs reference band."""
    if value <= 0:       return (0.0,  "FAILED",    CR)
    if value < lo:       return (0.15, "POOR",      CR)
    if value < typ:      return (0.45, "FAIR",      CY)
    if value < hi:       return (0.80, "GOOD",      CG)
    return                      (1.0,  "EXCELLENT", CG)

def _grade_latency(us: float, p99_typ: float, p99_max: float) -> Tuple[float, str, str]:
    """Lower latency is better — invert the grading scale."""
    if us <= 0:             return (0.0, "N/A",   FG3)
    if us <= p99_typ:       return (1.0, "EXCELLENT", CG)
    if us <= p99_typ * 2:   return (0.7, "GOOD",      CG)
    if us <= p99_max:       return (0.4, "FAIR",      CY)
    return                         (0.1, "POOR",      CR)


def _fio_available() -> bool:
    return shutil.which("fio") is not None

def _ensure_fio() -> Tuple[bool, str]:
    """Install fio via detected package manager. (ok, message)"""
    if _fio_available(): return (True, "fio already installed")
    pkg_by_mgr = {"emerge": "sys-block/fio"}
    for binary, args, _ in _PKG_MANAGERS:
        if not _which(binary): continue
        pkg = pkg_by_mgr.get(binary, "fio")
        cmd = [binary] + args + [pkg]
        if os.geteuid() != 0:
            if not _which("sudo"): continue
            cmd = ["sudo", "-n"] + cmd
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=180)
            if r.returncode == 0 and _fio_available():
                return (True, f"installed fio via {binary}")
        except Exception:
            continue
    return (False,
            "fio not found and auto-install failed.\n"
            "  Install manually:\n"
            "    Debian/Ubuntu:  sudo apt install fio\n"
            "    RHEL/Fedora:    sudo dnf install fio\n"
            "    Arch:           sudo pacman -S fio\n"
            "    Alpine:         sudo apk add fio\n"
            "    SUSE:           sudo zypper install fio")


def _detect_ioengine() -> str:
    """Pick the best fio ioengine that works at runtime.

    `fio --enghelp` lists compile-time engines but the kernel may not
    support them (older kernels lack io_uring). So we probe each with a
    1-second test and return the first one that yields iops > 0.
    """
    probe_file = f"/tmp/checkdisk_engprobe_{os.getpid()}"
    try:
        for engine in ("io_uring", "libaio", "psync"):
            try:
                with open(probe_file, "wb") as f:
                    f.write(b"\0" * (4 * 1024 * 1024))
                r = subprocess.run(
                    ["fio",
                     f"--ioengine={engine}",
                     "--name=probe",
                     f"--filename={probe_file}",
                     "--size=4M",
                     "--rw=read",
                     "--bs=4k",
                     "--runtime=1",
                     "--time_based",
                     "--direct=1",
                     "--output-format=json"],
                    capture_output=True, text=True, timeout=10)
                if r.returncode == 0:
                    try:
                        data = json.loads(r.stdout)
                        if data["jobs"][0]["read"]["iops"] > 0:
                            return engine
                    except Exception:
                        continue
            except Exception:
                continue
    finally:
        try: os.unlink(probe_file)
        except Exception: pass
    return "psync"   # last-resort fallback — works in every Linux kernel


def _iops_target_for(device: str) -> Tuple[str, str, int, str]:
    """Pick where to test. Returns (path, mode, size, info).

    mode='raw': test raw block device, read-only.
    mode='file': test a temp file on the largest writable mount.
    """
    mounts = _get_mounted_partitions(device)
    if not mounts:
        size_b = _get_disk_size_bytes(device)
        # 8 GiB region defeats SSD SLC cache on most consumer drives.
        region_size = min(size_b, 8 * 1024**3) if size_b else 8 * 1024**3
        return (device, "raw", region_size,
                f"raw device · {human_size(region_size)} test region")

    # Pick the mount with the most free space
    best_mount, best_free = None, 0
    for mp in mounts:
        try:
            u = psutil.disk_usage(mp)
            if u.free > best_free:
                best_free = u.free
                best_mount = mp
        except Exception: continue
    if best_mount is None:
        return (device, "raw", _get_disk_size_bytes(device), "raw device fallback")

    # 1–4 GiB test file; minimum 1 GiB to defeat SLC cache.
    test_size = min(4 * 1024**3, int(best_free * 0.10))
    test_size = max(1 * 1024**3, test_size)
    target    = os.path.join(best_mount, f".checkdisk_iotest_{os.getpid()}.tmp")
    return (target, "file", test_size,
            f"file on {best_mount} · {human_size(test_size)} test file")


def _bg_iops_test(state: "_State", disk: DiskInfo):
    """Run sequential / random-IOPS / latency benchmark on disk via fio.

    Runs in a background thread; updates state.iops_* for the UI.
    ~140 s total. Read-only; uses a temp file when the target has
    mounted partitions."""
    device = disk.device

    with state.lock:
        state.iops_running      = True
        state.iops_disk         = disk
        state.iops_phase        = "init"
        state.iops_phase_label  = "Preparing test…"
        state.iops_phase_num    = 0
        state.iops_phase_total  = 3
        state.iops_pct          = 0
        state.iops_overall      = 0
        state.iops_info         = "checking fio"
        state.iops_live         = ""
        state.iops_result       = None
        state.iops_cancel       = False
        state.clean_status[device] = "iops"
        state._dirty = True
    state.refresh_ev.set()

    # Ensure fio
    if not _fio_available():
        with state.lock:
            state.iops_phase_label = "Installing fio…"
            state.iops_info = "this is a one-time setup"
            state._dirty = True
        state.refresh_ev.set()
        ok, msg = _ensure_fio()
        if not ok:
            _iops_finish(state, device, error=msg); return

    # Pick target and ioengine
    target, mode, test_size, target_info = _iops_target_for(device)
    engine = _detect_ioengine()

    with state.lock:
        state.iops_info = f"using {engine} · {target_info}"
        state._dirty = True
    state.refresh_ev.set()

    # Check we can actually read the target
    try:
        if mode == "raw":
            with open(target, "rb") as f:
                f.read(4096)
        else:
            tdir = os.path.dirname(target)
            if not os.access(tdir, os.W_OK):
                _iops_finish(state, device,
                             error=f"No write access to {tdir}\n  "
                                   f"Run with sudo or pick a different target.")
                return
    except PermissionError:
        _iops_finish(state, device,
                     error=f"Permission denied opening {target}\n  "
                           f"Run with: sudo checkdisk all"); return
    except FileNotFoundError:
        _iops_finish(state, device, error=f"Device {target} not found"); return
    except Exception as e:
        _iops_finish(state, device, error=f"Pre-flight failed: {e}"); return

    # Probe O_DIRECT support — tmpfs/overlayfs/NFS reject it, falling back
    # to buffered I/O lets the benchmark still run with a cache caveat.
    direct_flag = 1
    try:
        probe_target = target if mode == "raw" else target + ".odirect_probe"
        if mode == "file":
            with open(probe_target, "wb") as f:
                f.write(b"\0" * (1024 * 1024))
        probe = subprocess.run(
            ["fio",
             f"--ioengine={engine}",
             "--name=odirect_probe",
             f"--filename={probe_target}",
             "--size=1M",
             "--rw=read",
             "--bs=4k",
             "--runtime=1",
             "--time_based",
             "--direct=1",
             "--output-format=json"],
            capture_output=True, text=True, timeout=10)
        ok = False
        if probe.returncode == 0:
            try:
                ok = json.loads(probe.stdout)["jobs"][0]["read"]["iops"] > 0
            except Exception:
                ok = False
        if not ok:
            direct_flag = 0   # fall back; cache effects will be noted in result
        if mode == "file":
            try: os.unlink(probe_target)
            except Exception: pass
    except Exception:
        direct_flag = 0

    # Three fio sub-jobs, run sequentially via --stonewall, one JSON output.
    out_file = f"/tmp/checkdisk_fio_{os.getpid()}.json"
    err_file = f"/tmp/checkdisk_fio_{os.getpid()}.err"
    job_file = f"/tmp/checkdisk_fio_{os.getpid()}.fio"

    job_content = f"""[global]
ioengine={engine}
direct={direct_flag}
filename={target}
size={test_size}
group_reporting=1
randrepeat=0
norandommap=1
time_based=1
# 5s ramp excludes cache-warming / queue-fill effects from the measurement.
ramp_time=5

[seq-bandwidth]
description=Sequential read bandwidth (large blocks)
rw=read
bs=1M
iodepth=32
numjobs=1
runtime=30
stonewall

[random-iops]
description=Random read IOPS (small blocks, deep queue)
rw=randread
bs=4k
iodepth=32
numjobs=4
runtime=60
stonewall

[random-latency]
description=Single-thread latency
rw=randread
bs=4k
iodepth=1
numjobs=1
runtime=30
stonewall
"""

    try:
        with open(job_file, "w") as f: f.write(job_content)
    except Exception as e:
        _iops_finish(state, device, error=f"Couldn't write job file: {e}"); return

    # NB: --output-format and --output must be CLI flags, not job-file keys —
    # fio rejects them with "Bad option" if placed inside [global].
    cmd = ["fio",
           "--output-format=json",
           f"--output={out_file}",
           job_file]

    # Total expected runtime: 5s ramp + 30s sequential + 5s ramp + 60s IOPS
    # + 5s ramp + 30s latency = 135s, plus ~5s startup/finalise = 140s.
    total_runtime = 140
    test_start    = time.time()

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                                stderr=open(err_file, "w"),
                                close_fds=True)
    except Exception as e:
        _iops_finish(state, device, error=f"Couldn't start fio: {e}"); return

    # Phase schedule: (phase_start_s, phase_end_s, phase_num, label).
    # Ramp + measurement windows combined so the progress UI tracks wall-clock.
    phase_schedule = [
        (  0,  35,  1, "Test 1/3 — Sequential read bandwidth (30s + 5s ramp)"),
        ( 35, 100,  2, "Test 2/3 — Random IOPS (60s + 5s ramp, 4 KiB QD=32)"),
        (100, 135,  3, "Test 3/3 — Latency (30s + 5s ramp, 4 KiB QD=1)"),
        (135, 140,  3, "Finalising results…"),
    ]

    while proc.poll() is None:
        with state.lock:
            if state.iops_cancel:
                try: proc.terminate()
                except Exception: pass
                try: proc.wait(timeout=5)
                except Exception:
                    try: proc.kill()
                    except Exception: pass
                break

        elapsed = time.time() - test_start
        # Pick current phase from schedule
        cur_label = phase_schedule[-1][3]
        cur_num   = phase_schedule[-1][2]
        phase_pct = 99
        for ps, pe, pn, pl in phase_schedule:
            if ps <= elapsed < pe:
                cur_label = pl; cur_num = pn
                phase_pct = int((elapsed - ps) * 100 / max(1, pe - ps))
                break

        overall = min(99, int(elapsed * 100 / total_runtime))
        remain  = max(0, int(total_runtime - elapsed))

        with state.lock:
            state.iops_phase_label = cur_label
            state.iops_phase_num   = cur_num
            state.iops_pct         = phase_pct
            state.iops_overall     = overall
            state.iops_info        = (f"about {remain}s remaining   ·   "
                                      f"engine: {engine}   ·   "
                                      f"target: {os.path.basename(target)}")
            state._dirty = True
        state.refresh_ev.set()
        time.sleep(0.4)

    # Drain
    try: proc.wait(timeout=10)
    except Exception:
        try: proc.kill()
        except Exception: pass

    # Cleanup temp file (only if we created one)
    if mode == "file":
        try: os.unlink(target)
        except Exception: pass

    # Cancelled?
    with state.lock:
        was_cancelled = state.iops_cancel
    if was_cancelled:
        try: os.unlink(out_file)
        except Exception: pass
        try: os.unlink(err_file)
        except Exception: pass
        try: os.unlink(job_file)
        except Exception: pass
        _iops_finish(state, device, error="Cancelled by user"); return

    # Parse fio JSON output
    parsed = None
    try:
        with open(out_file) as f: parsed = json.load(f)
    except Exception as e:
        # Read stderr for the actual fio error message
        err_text = ""
        try:
            with open(err_file) as f: err_text = f.read().strip()
        except Exception: pass
        # Trim & make readable
        if err_text:
            lines = [ln for ln in err_text.splitlines()
                     if ln.strip() and not ln.startswith("Jobs:")]
            err_text = "\n  ".join(lines[:6])
        msg = (f"fio test failed.\n  "
               f"{err_text or 'No diagnostic output captured.'}\n  "
               f"Couldn't read results: {e}")
        _iops_finish(state, device, error=msg); return
    finally:
        for f in (out_file, err_file, job_file):
            try: os.unlink(f)
            except Exception: pass

    # Extract per-test metrics
    seq  = _fio_extract_job(parsed, "seq-bandwidth")
    rand = _fio_extract_job(parsed, "random-iops")
    lat  = _fio_extract_job(parsed, "random-latency")

    final = {
        "device":       device,
        "model":        disk.model,
        "interface":    disk.interface,
        "media":        _media_type(disk),
        "size_human":   disk.size_human,
        "test_target":  target_info,
        "test_engine":  engine,
        "test_direct":  bool(direct_flag),
        "seq_mbps":     seq.get("bw_kibs", 0) * 1024 / 1_000_000,
        "rand_iops":    int(rand.get("iops", 0)),
        "rand_mbps":    rand.get("bw_kibs", 0) * 1024 / 1_000_000,
        "lat_avg_us":   lat.get("lat_avg_us", 0),
        "lat_p50_us":   lat.get("lat_p50_us", 0),
        "lat_p95_us":   lat.get("lat_p95_us", 0),
        "lat_p99_us":   lat.get("lat_p99_us", 0),
        "lat_p999_us":  lat.get("lat_p999_us", 0),
        "error":        None,
    }
    _iops_finish(state, device, result=final)


def _iops_finish(state: "_State", device: str,
                 result: Optional[Dict] = None, error: Optional[str] = None):
    """Helper: publish final IOPS state in one place."""
    with state.lock:
        state.iops_running = False
        if error:
            state.iops_phase  = "error"
            state.iops_result = {"error": error}
        else:
            state.iops_phase  = "done"
            state.iops_result = result
        state.iops_pct     = 100
        state.iops_overall = 100
        state.clean_status.pop(device, None)
        state._dirty = True
    state.refresh_ev.set()


def _save_iops_report(state: "_State") -> Optional[str]:
    """Save IOPS result as JSON + TXT in ~/checkdisk-reports/.

    Returns the TXT path, or None on failure. Under sudo, writes to the
    invoking user's home (not /root) and chowns the files back to them.
    """
    with state.lock:
        disk = state.iops_disk
        r    = dict(state.iops_result or {})
    if not r or r.get("error") or disk is None:
        return None

    # Pick home dir, handling sudo so reports land in the real user's home
    try:
        home = os.path.expanduser("~")
        sudo_user = os.environ.get("SUDO_USER")
        if sudo_user and os.geteuid() == 0:
            try:
                import pwd
                home = pwd.getpwnam(sudo_user).pw_dir
            except Exception:
                pass
        base = os.path.join(home, "checkdisk-reports")
        os.makedirs(base, exist_ok=True)

        if sudo_user and os.geteuid() == 0:
            try:
                import pwd
                p = pwd.getpwnam(sudo_user)
                os.chown(base, p.pw_uid, p.pw_gid)
            except Exception:
                pass
    except Exception:
        return None

    # Filename: IOPS_<serial>_<date>_<time>.{txt,json}
    serial = (disk.serial or "").strip()
    if not serial or serial in ("N/A", "-", "unknown"):
        serial = os.path.basename(disk.device).replace("/", "_") or "disk"
    serial = re.sub(r"[^A-Za-z0-9._-]", "_", serial)

    date_str = time.strftime("%Y-%m-%d")
    time_str = time.strftime("%H-%M-%S")
    base_name = f"IOPS_{serial}_{date_str}_{time_str}"
    json_fp = os.path.join(base, f"{base_name}.json")
    txt_fp  = os.path.join(base, f"{base_name}.txt")

    ref = _io_reference(disk)
    lo_m, typ_m, hi_m, lo_i, typ_i, hi_i, p99_max_us, p99_typ_us, label = ref

    # Compute grades for the report
    seq_pos,  seq_lbl,  _ = _grade_score(r.get("seq_mbps",0),  lo_m, typ_m, hi_m)
    iops_pos, iops_lbl, _ = _grade_score(r.get("rand_iops",0), lo_i, typ_i, hi_i)
    lat_pos,  lat_lbl,  _ = _grade_latency(r.get("lat_p99_us",0), p99_typ_us, p99_max_us)
    overall_pos = iops_pos * 0.40 + lat_pos * 0.30 + seq_pos * 0.30
    if   overall_pos >= 0.85: overall_lbl = "EXCELLENT"
    elif overall_pos >= 0.60: overall_lbl = "GOOD"
    elif overall_pos >= 0.35: overall_lbl = "FAIR"
    else:                     overall_lbl = "POOR"

    # JSON export
    json_payload = {
        "timestamp":   time.strftime("%Y-%m-%d %H:%M:%S %z"),
        "hostname":    os.uname().nodename if hasattr(os, "uname") else "",
        "device":      disk.device,
        "model":       disk.model,
        "serial":      disk.serial,
        "firmware":    disk.firmware,
        "interface":   disk.interface,
        "media":       _media_type(disk),
        "size_bytes":  disk.size_bytes,
        "size_human":  disk.size_human,
        "reference":   {
            "class":          label,
            "seq_mbps_min":   lo_m, "seq_mbps_typ":   typ_m, "seq_mbps_max":  hi_m,
            "iops_min":       lo_i, "iops_typ":       typ_i, "iops_max":      hi_i,
            "lat_p99_typ_us": p99_typ_us, "lat_p99_max_us": p99_max_us,
        },
        "measurements": {
            "seq_mbps":     r.get("seq_mbps",   0.0),
            "rand_iops":    r.get("rand_iops",  0),
            "lat_avg_us":   r.get("lat_avg_us", 0.0),
            "lat_p50_us":   r.get("lat_p50_us", 0.0),
            "lat_p95_us":   r.get("lat_p95_us", 0.0),
            "lat_p99_us":   r.get("lat_p99_us", 0.0),
            "lat_p999_us":  r.get("lat_p999_us",0.0),
        },
        "grades": {
            "sequential":  seq_lbl,
            "random_iops": iops_lbl,
            "latency_p99": lat_lbl,
            "overall":     overall_lbl,
        },
        "methodology": {
            "tool":       "fio " + _fio_version(),
            "engine":     r.get("test_engine", ""),
            "direct_io":  bool(r.get("test_direct", True)),
            "target":     r.get("test_target",  ""),
            "standard":   "SNIA SSS-PTS derived",
            "ramp_time_sec":   5,
            "sequential_runtime_sec": 30,
            "iops_runtime_sec":       60,
            "latency_runtime_sec":    30,
        },
    }
    try:
        with open(json_fp, "w") as f: json.dump(json_payload, f, indent=2)
    except Exception:
        return None

    # TXT report — human-readable, attachable to tickets
    def fmt_iops(n):
        n = int(n)
        if n >= 1_000_000: return f"{n/1_000_000:.2f} M"
        if n >= 1_000:     return f"{n/1_000:.1f} K"
        return str(n)
    def fmt_us(us):
        if us <= 0: return "—"
        if us < 1000: return f"{us:.0f} µs"
        return            f"{us/1000:.2f} ms"

    lines = [
        "═" * 72,
        "  checkdisk  —  I/O & IOPS benchmark report",
        "═" * 72,
        f"  Generated   {time.strftime('%Y-%m-%d %H:%M:%S %z')}",
        f"  Hostname    {json_payload['hostname']}",
        "",
        "  Device",
        f"    Path         {disk.device}",
        f"    Model        {disk.model}",
        f"    Serial       {disk.serial}",
        f"    Firmware     {disk.firmware}",
        f"    Interface    {disk.interface}",
        f"    Media type   {_media_type(disk)}",
        f"    Capacity     {disk.size_human}",
        "",
        f"  Reference class: {label}",
        f"    Sequential   {lo_m}–{hi_m} MB/s",
        f"    Random IOPS  {fmt_iops(lo_i)}–{fmt_iops(hi_i)}",
        f"    Latency p99  ≤ {int(p99_typ_us)} µs good, ≤ {int(p99_max_us)} µs acceptable",
        "",
        "  Measurements",
        f"    Sequential   {r.get('seq_mbps', 0):>10.1f} MB/s    [{seq_lbl}]",
        f"    Random IOPS  {fmt_iops(r.get('rand_iops', 0)):>10}          [{iops_lbl}]",
        f"    Latency p50  {fmt_us(r.get('lat_p50_us', 0)):>10}",
        f"    Latency p95  {fmt_us(r.get('lat_p95_us', 0)):>10}",
        f"    Latency p99  {fmt_us(r.get('lat_p99_us', 0)):>10}          [{lat_lbl}]",
        f"    Latency p999 {fmt_us(r.get('lat_p999_us', 0)):>10}",
        "",
        f"  Overall verdict:  {overall_lbl}",
        "",
        "  Methodology",
        f"    Tool         fio {_fio_version()}",
        f"    Engine       {r.get('test_engine','')}",
        f"    Cache        {'O_DIRECT (bypassed)' if r.get('test_direct', True) else 'buffered'}",
        f"    Target       {r.get('test_target','')}",
        f"    Ramp time    5 s per phase (excluded from measurements)",
        f"    Runtime      30 s sequential  /  60 s IOPS  /  30 s latency",
        f"    Standard     SNIA SSS-PTS derived",
        "",
        "═" * 72,
    ]
    try:
        with open(txt_fp, "w") as f: f.write("\n".join(lines) + "\n")
    except Exception:
        return json_fp   # JSON saved, text failed — still return something

    # Chown files to invoking user if running as root via sudo, so the
    # user can open/edit/delete them without needing sudo afterwards.
    if sudo_user and os.geteuid() == 0:
        try:
            import pwd
            p = pwd.getpwnam(sudo_user)
            for fp in (json_fp, txt_fp):
                try: os.chown(fp, p.pw_uid, p.pw_gid)
                except Exception: pass
        except Exception:
            pass

    return txt_fp


def _fio_version() -> str:
    try:
        r = subprocess.run(["fio", "--version"], capture_output=True, text=True, timeout=3)
        return r.stdout.strip() or r.stderr.strip()
    except Exception:
        return "unknown"


def _fio_extract_job(data: Dict, job_name: str) -> Dict:
    """Pull one job's metrics out of an fio JSON output.
    Falls back to the first job whose name matches the prefix."""
    if not data or not data.get("jobs"): return {}
    job = None
    for j in data["jobs"]:
        if j.get("jobname", "") == job_name or j.get("jobname", "").startswith(job_name):
            job = j; break
    if job is None:
        # Sometimes fio uses just the first job — fall back gracefully
        job = data["jobs"][0]
    blk  = job.get("read", {}) if job.get("read", {}).get("io_bytes", 0) > 0 else job.get("write", {})
    clat = blk.get("clat_ns", {}) or {}
    pct  = clat.get("percentile", {}) or {}
    ns2us = lambda v: (v / 1000.0) if v else 0.0
    return {
        "iops":         blk.get("iops", 0.0),
        "bw_kibs":      blk.get("bw", 0),
        "lat_avg_us":   ns2us(clat.get("mean", 0)),
        "lat_min_us":   ns2us(clat.get("min",  0)),
        "lat_max_us":   ns2us(clat.get("max",  0)),
        "lat_p50_us":   ns2us(pct.get("50.000000", 0)),
        "lat_p95_us":   ns2us(pct.get("95.000000", 0)),
        "lat_p99_us":   ns2us(pct.get("99.000000", 0)),
        "lat_p999_us":  ns2us(pct.get("99.900000", 0)),
    }


# ─── IOPS progress overlay ────────────────────────────────────────────────────

def _iops_progress_panel(state: "_State", t_: float) -> Panel:
    """Compact progress card — single accent-coloured progress bar (not the
    red→green status gradient), tight left-aligned rows, no big gaps."""
    with state.lock:
        disk    = state.iops_disk
        label   = state.iops_phase_label or "starting…"
        info    = state.iops_info
        overall = state.iops_overall

    spin   = "⣾⣽⣻⢿⡿⣟⣯⣷"[int(t_ * 8) % 8]
    accent = "#88c0d0"

    # Tight 2-column grid: narrow label (10), value takes the rest.
    # No right-justification — everything left-aligned so the eye doesn't
    # have to jump across a 30-character gap.
    grid = Table.grid(padding=(0, 2), expand=False)
    grid.add_column(style=FG3, no_wrap=True, width=10)
    grid.add_column(no_wrap=True, overflow="ellipsis")

    # Device identity
    if disk:
        grid.add_row("Device", Text(disk.device, style=f"bold {accent}"))
        grid.add_row("Model",  Text(disk.model,  style=FG1))
        grid.add_row("Class",
                     Text(f"{disk.interface} · {_media_type(disk)}  ·  {disk.size_human}",
                          style=FG1))

    grid.add_row("", "")   # spacer

    # Current status + phase
    status_text = Text()
    status_text.append(f"{spin} ", style=f"bold {accent}")
    status_text.append("RUNNING", style=f"bold {accent}")
    grid.add_row("Status", status_text)
    grid.add_row("Phase",  Text(label, style=FG0, overflow="ellipsis"))

    grid.add_row("", "")   # spacer

    # Progress bar — wider, with the proper progress renderer
    pbar = Text()
    pbar.append_text(_progress_bar(overall, 38, t_))
    pbar.append(f"  {overall:>3} %", style=f"bold {accent}")
    grid.add_row("Progress", pbar)

    # Status info line (if any)
    if info:
        grid.add_row("", Text(info, style=FG3, overflow="ellipsis"))

    return Panel(
        grid,
        title        = f"[bold {accent}]   I/O  &  IOPS  BENCHMARK   [/bold {accent}]",
        subtitle     = f"[{FG3}]  safe · read-only  ·  Esc / Q to abort  [/{FG3}]",
        border_style = accent,
        box          = box.ROUNDED,
        padding      = (1, 3),
    )


# ─── IOPS result overlay ──────────────────────────────────────────────────────

def _iops_result_panel(state: "_State") -> Panel:
    """Render the final I/O test result card."""
    with state.lock:
        disk = state.iops_disk
        r    = state.iops_result or {}

    # Error path
    if r.get("error"):
        err = Text()
        err.append(f"\n  {SYM_CRIT}  Test failed\n\n", style=f"bold {CR}")
        for line in str(r["error"]).split("\n"):
            err.append(f"  {line}\n", style=FG1)
        err.append("\n  Esc · Enter · Space · Q  to close\n", style=FG3)
        return Panel(err,
                     title=f"[bold {CR}]  I/O TEST RESULT  [/bold {CR}]",
                     border_style=CR, box=box.ROUNDED, padding=(0, 2))

    # Grading
    ref = _io_reference(disk) if disk else _IO_REFERENCE[("SATA", "UNKNOWN")]
    lo_m, typ_m, hi_m, lo_i, typ_i, hi_i, p99_max_us, p99_typ_us, label = ref
    seq_mbps  = r.get("seq_mbps",  0.0)
    rand_iops = r.get("rand_iops", 0)
    lat_p99   = r.get("lat_p99_us",0)

    seq_pos,  seq_lbl,  seq_c  = _grade_score(seq_mbps,  lo_m, typ_m, hi_m)
    iops_pos, iops_lbl, iops_c = _grade_score(rand_iops, lo_i, typ_i, hi_i)
    lat_pos,  lat_lbl,  lat_c  = _grade_latency(lat_p99, p99_typ_us, p99_max_us)

    overall_pos = iops_pos * 0.40 + lat_pos * 0.30 + seq_pos * 0.30
    if   overall_pos >= 0.85: overall_lbl, overall_c = "EXCELLENT", CG
    elif overall_pos >= 0.60: overall_lbl, overall_c = "GOOD",      CG
    elif overall_pos >= 0.35: overall_lbl, overall_c = "FAIR",      CY
    else:                     overall_lbl, overall_c = "POOR",      CR

    def fmt_iops(n: float) -> str:
        n = int(n)
        if n >= 1_000_000: return f"{n/1_000_000:.2f} M"
        if n >= 1_000:     return f"{n/1_000:.1f} K"
        return str(n)

    def fmt_us(us: float) -> str:
        if us <= 0:   return "—"
        if us < 1000: return f"{us:.0f} µs"
        return             f"{us/1000:.2f} ms"

    # 4-column master grid: LABEL / VALUE / BAR / GRADE
    grid = Table.grid(padding=(0, 2), expand=False)
    grid.add_column(style=FG3, no_wrap=True, width=14)
    grid.add_column(no_wrap=True, min_width=16, max_width=30, overflow="ellipsis")
    grid.add_column(no_wrap=True, width=24)
    grid.add_column(no_wrap=True, width=11)

    def spacer():
        grid.add_row("", "", "", "")

    # STATUS bar (red→yellow→green) — semantically correct for grading metrics
    def status_bar(pos: float) -> Text:
        t = Text()
        t.append_text(_gradient_bar(pos * 100, 22, invert=True))
        return t

    # Section 1 — device identity
    if disk:
        grid.add_row("Device",
                     Text(disk.device, style=f"bold {FG0}"),
                     "", "")
        grid.add_row("Model",
                     Text(disk.model[:38], style=FG1),
                     "", "")
        size_info = f"{label}  ·  {disk.size_human}"
        grid.add_row("Class",
                     Text(size_info, style=FG1),
                     "", "")

    spacer()

    # Section 2 — metric header
    grid.add_row(
        Text("METRIC",   style=f"bold {FG3}"),
        Text("VALUE",    style=f"bold {FG3}"),
        Text("PROGRESS", style=f"bold {FG3}"),
        Text("GRADE",    style=f"bold {FG3}"),
    )

    # The three headline numbers
    grid.add_row(
        Text("Sequential", style=FG2),
        Text(f"{seq_mbps:.0f} MB/s", style=f"bold {seq_c}"),
        status_bar(seq_pos),
        Text(seq_lbl, style=f"bold {seq_c}"),
    )
    grid.add_row(
        Text("Random IOPS", style=FG2),
        Text(fmt_iops(rand_iops), style=f"bold {iops_c}"),
        status_bar(iops_pos),
        Text(iops_lbl, style=f"bold {iops_c}"),
    )
    grid.add_row(
        Text("Latency p99", style=FG2),
        Text(fmt_us(lat_p99), style=f"bold {lat_c}"),
        status_bar(lat_pos),
        Text(lat_lbl, style=f"bold {lat_c}"),
    )

    spacer()

    # Section 3 — expected-for-class reference (compact one-liner per metric)
    grid.add_row("Expected seq",
                 Text(f"{lo_m}–{hi_m}", style=FG2),
                 Text("MB/s", style=FG4), "")
    grid.add_row("Expected iops",
                 Text(f"{fmt_iops(lo_i)}–{fmt_iops(hi_i)}", style=FG2),
                 Text("ops", style=FG4), "")
    grid.add_row("Expected p99",
                 Text(f"≤ {int(p99_typ_us)} µs", style=FG2),
                 Text(f"({int(p99_max_us)} max)", style=FG4), "")

    spacer()

    # Section 4 — VERDICT.  Bar here uses the VERDICT's own colour (CG/CY/CR)
    # instead of the red→yellow→green status gradient, because "GOOD" being
    # shown with a red-tinted bar in front of it is contradictory.
    def solid_bar(pos: float, colour: str) -> Text:
        """Solid-colour progress bar — the verdict colour, darker tail."""
        t = Text()
        if pos < 0: pos = 0
        if pos > 1: pos = 1
        full = int(round(pos * 22 * 8)) // 8
        partial = int(round(pos * 22 * 8)) % 8
        empty = 22 - full - (1 if partial else 0)
        for _ in range(full):
            t.append("█", style=f"bold {colour}")
        if partial and full < 22:
            t.append(_BAR[partial], style=f"bold {colour}")
        t.append("░" * max(0, empty), style="#3b4252")
        return t

    grid.add_row(
        Text("VERDICT", style=f"bold {FG3}"),
        Text(overall_lbl, style=f"bold {overall_c}"),
        solid_bar(overall_pos, overall_c),
        "",
    )

    # Verdict one-liner (spans full width, separate from the grid so it
    # isn't constrained to a 14-char value column)
    verdict_msg = {
        "EXCELLENT": "Meets or exceeds top of class  ·  ready for production",
        "GOOD":      "Performs as expected for its class  ·  suitable for production",
        "FAIR":      "Below typical for this class  ·  review before deploying",
        "POOR":      "Significantly below spec  ·  do NOT deploy without investigation",
    }[overall_lbl]

    # Section 5 — methodology (compact single line)
    cache = "O_DIRECT" if r.get("test_direct", True) else "buffered"
    method_txt = (f"fio {r.get('test_engine','libaio')}  ·  {cache}  ·  "
                  f"{r.get('test_target','raw device')}")

    # Assemble — grid + free-form rows below for the verdict sentence and method
    body = Table.grid(padding=0, expand=False)
    body.add_column()
    body.add_row(grid)
    body.add_row(Text(""))                           # blank spacer
    body.add_row(Text(verdict_msg, style=FG1))
    body.add_row(Text(""))
    body.add_row(Text(f"Method:  {method_txt}", style=FG3))

    saved = r.get("saved_path")
    footer_hint = (
        f"[{CG}]  ✓ saved to  {saved}  [/{CG}]"
        if saved
        else f"[{FG3}]  S save report  ·  Esc  Enter  Space  Q  close  [/{FG3}]"
    )

    return Panel(
        body,
        title        = f"[bold {overall_c}]   I/O TEST RESULT   —   {overall_lbl}   [/bold {overall_c}]",
        subtitle     = footer_hint,
        border_style = overall_c,
        box          = box.ROUNDED,
        padding      = (1, 3),
    )


# ─── Footer ────────────────────────────────────────────────────────────────────

def _footer() -> Text:
    pairs = [("↑↓ jk","navigate"), ("C","clean"), ("I","iops test"),
             ("S","save report"), ("R","refresh"), ("Q","quit")]
    t = Text()
    t.append("  ")
    for i, (k, d) in enumerate(pairs):
        if i: t.append("   ", style=FG4)
        t.append(f" {k} ", style="bold #2e3440 on #4c566a")   # Nord kbd style
        t.append(f" {d}", style=FG3)
    t.append("  ")
    return t


# ─── Full frame compositor ─────────────────────────────────────────────────────

def _render_frame(state: "_State", t_: float, cols: int, rows: int) -> str:
    from rich.console import Group
    ts  = time.strftime("%Y-%m-%d  %H:%M:%S")
    hdr = _header_panel(state, ts)
    tbl = _disk_panel(state, t_)
    ftr = _footer()

    with state.lock:
        modal        = state.modal_disk
        mbtn         = state.modal_btn
        iops_running = state.iops_running
        iops_result  = state.iops_result
        iops_disk    = state.iops_disk

    # Pick which (if any) overlay to show
    overlay = None
    if modal is not None:
        overlay = _modal_panel(modal, mbtn)
    elif iops_running:
        overlay = _iops_progress_panel(state, t_)
    elif iops_result is not None and iops_disk is not None:
        overlay = _iops_result_panel(state)

    if overlay is not None:
        group = Group(hdr, Align(overlay, align="center"), tbl, ftr)
    else:
        group = Group(hdr, tbl, ftr)

    # Render at cols-1 to avoid terminal auto-wrap on the last column
    # (which was clipping our right borders).
    render_width = max(60, cols - 1)

    buf = io.StringIO()
    tmp = Console(file=buf, width=render_width, height=rows, force_terminal=True,
                  color_system="truecolor", highlight=False, markup=True, legacy_windows=False)
    tmp.print(group)
    raw = buf.getvalue()

    # Ghost-proof each line so stale chars from a previous longer frame
    # are wiped as the new frame writes.
    return "\n".join(ln + "\033[K" for ln in raw.split("\n"))


# ─── Detail view ──────────────────────────────────────────────────────────────

def cmd_device(device_path: str):
    _warn_requirements()
    if device_path.startswith("/dev/"): dev_name = device_path[5:]
    else: dev_name, device_path = device_path, f"/dev/{device_path}"
    if not os.path.exists(device_path):
        console.print(f"\n[bold {CR}]  ✕  Device not found: {device_path}[/bold {CR}]\n")
        sys.exit(1)
    with console.status(f"[{FG2}]  Reading SMART data …[/{FG2}]",
                        spinner="dots", spinner_style=BOX_TITLE):
        try: info = fetch_disk(dev_name)
        except Exception as e:
            console.print(f"[{CR}]  Error: {e}[/{CR}]"); sys.exit(1)
    _render_detail(info)

def _kv(label: str, value, vs: str = FG1) -> tuple:
    return (Text(label, style=FG2),
            value if isinstance(value, Text) else Text(str(value), style=vs))


def _render_detail(info: DiskInfo):
    console.print()

    # Header
    hdr_row = Table(show_header=False, box=None, padding=0, expand=True)
    hdr_row.add_column("l", ratio=3); hdr_row.add_column("r", ratio=1, justify="right")
    lft = Text()
    lft.append(" CHECKDISK ", style=FG0)
    lft.append("›  Disk Detail View", style=FG2)
    hdr_row.add_row(lft, _status_cell(info))

    sub_row = Text()
    sub_row.append(f"  {info.device}", style=FG0)
    if info.model != "Unknown":
        sub_row.append(f"  ·  {info.model}", style=FG1)
    sub_row.append(f"  ·  {info.size_human}", style=FG0)
    sub_row.append("  ·  ", style=FG4)
    sub_row.append_text(_iface_cell(info.interface))

    hdr_body = Table(show_header=False, box=None, padding=(0,0), expand=True)
    hdr_body.add_column("c")
    hdr_body.add_row(hdr_row); hdr_body.add_row(Rule(style=BOX_DIV)); hdr_body.add_row(sub_row)
    console.print(btop_panel(hdr_body, f"{info.device}"))
    console.print()

    # Score cards
    def score_card(label: str, pct: int, invert: bool, sublabel: str) -> Panel:
        c = _hc(pct)
        inner = Table(show_header=False, box=None, padding=(0,1), expand=True)
        inner.add_column("c", justify="center")
        inner.add_row(Text(f"{pct}%" if pct>=0 else "N/A", style=f"bold {c}", justify="center"))
        b = Text(justify="center")
        b.append("  "); b.append_text(_gradient_bar(pct, 14, invert)); b.append("  ")
        inner.add_row(b)
        inner.add_row(Text(sublabel, style=f"bold {c}", justify="center"))
        return btop_panel(inner, label)

    def temp_card() -> Panel:
        inner = Table(show_header=False, box=None, padding=(0,1), expand=True)
        inner.add_column("c", justify="center")
        if info.temp_c is not None:
            c = _tc(info.temp_c); lbl = temp_label(info.temp_c)
            t_pct = max(0.0, min(100.0, ((info.temp_c-15)/60.0)*100))
            inner.add_row(Text(f"{info.temp_c}°C", style=f"bold {c}", justify="center"))
            b = Text(justify="center")
            b.append("  "); b.append_text(_gradient_bar(t_pct, 14, invert=False)); b.append("  ")
            inner.add_row(b)
            inner.add_row(Text(lbl, style=f"bold {c}", justify="center"))
            # Sparkline history
            if info.temp_history:
                hist = info.temp_history
                lo, hi = min(hist), max(hist)
                span = max(1, hi-lo)
                sp_chars = " ▁▂▃▄▅▆▇█"
                spark = Text(" history: ", style=FG3, justify="center")
                for v in hist[-12:]:
                    idx = int(((v-lo)/span)*8)
                    spark.append(sp_chars[idx], style=_tc(v))
                inner.add_row(spark)
        else:
            inner.add_row(Text("N/A",       style=FG3, justify="center"))
            inner.add_row(Text("─"*14,      style=FG4, justify="center"))
            inner.add_row(Text("NO SENSOR", style=FG3, justify="center"))
        return btop_panel(inner, "temperature")

    console.print(Columns([
        score_card("health",      info.health_pct, invert=True,  sublabel=score_label(info.health_pct)),
        score_card("performance", info.perf_pct,   invert=True,  sublabel=score_label(info.perf_pct)),
        temp_card(),
    ], equal=True, expand=True))
    console.print()

    # Info grid
    def mkv(rows: list) -> Table:
        t = Table(show_header=False, box=None, padding=(0,2), expand=True)
        t.add_column("k", style=FG2, min_width=20, no_wrap=True)
        t.add_column("v", style=FG1)
        for r in rows:
            if r is None: t.add_row("", "")
            else:         t.add_row(*r)
        return t

    ss_t = {"PASSED": Text("✓ PASSED", style=f"bold {CG}"),
            "FAILED": Text("✕ FAILED", style=f"bold {CR}")}.get(
            info.smart_status, Text(f"? {info.smart_status}", style=FG3))

    def flag(n: int) -> Text:
        t = Text()
        if n==0: t.append(f"● {n}", style=f"bold {CG}")
        else:    t.append(f"▲ {n}", style=f"bold {CR}")
        return t

    info_rows = [
        _kv("Device",        info.device, FG0),
        _kv("Model",         info.model, FG1),
        _kv("Serial",        info.serial, FG1),
        _kv("Firmware",      info.firmware, FG1),
        _kv("Interface",     _iface_cell(info.interface)),
        _kv("Capacity",      info.size_human, FG0),
        None,
        _kv("SMART Status",  ss_t),
        _kv("Power On",      f"{_poh_fmt(info.power_on_hours)}  ({info.power_on_hours:,} hrs)" if info.power_on_hours else "—", FG1),
        _kv("Power Cycles",  str(info.power_cycles) if info.power_cycles else "—", FG1),
    ]
    health_rows = [
        _kv("Reallocated Sectors",  flag(info.reallocated_sectors)),
        _kv("Pending Sectors",      flag(info.pending_sectors)),
        _kv("Uncorrectable Errors", flag(info.uncorrectable_errors)),
        _kv("SMART Error Log",      flag(info.smart_error_count)),
        None, None,
    ]

    if info.total_bytes>0:
        c = _uc(info.used_pct)
        br = Text()
        br.append_text(_gradient_bar(info.used_pct, 16, invert=False))
        br.append(f"  {info.used_pct:.1f}%", style=f"bold {c}")
        usage_rows = [
            _kv("Used",  Text(human_size(info.used_bytes),  style=f"bold {c}")),
            _kv("Free",  Text(human_size(info.free_bytes),  style=f"bold {CG}")),
            _kv("Total", Text(human_size(info.total_bytes), style=FG1)),
            None,
            _kv("Usage", br),
            None,
        ]
    else:
        usage_rows = [_kv("Mounted", Text("No partitions mounted", style=FG3))]

    console.print(Columns([
        btop_panel(mkv(info_rows),   "device info"),
        btop_panel(mkv(health_rows), "health counters"),
        btop_panel(mkv(usage_rows),  "storage"),
    ], equal=True, expand=True))
    console.print()

    # SMART table
    if info.smart_attrs:
        at = Table(show_header=True, header_style=f"bold {BOX_TITLE}",
                   box=box.SIMPLE_HEAVY, border_style=BOX_DIV,
                   expand=True, padding=(0,1), row_styles=["", f"on color(233)"])
        at.add_column("ID",     justify="right", width=5,     style=FG3)
        at.add_column("ATTRIBUTE",  min_width=24,              style=FG1)
        at.add_column("VALUE",  justify="right", width=7,     style=FG1)
        at.add_column("WORST",  justify="right", width=7,     style=FG3)
        at.add_column("THRESH", justify="right", width=8,     style=FG3)
        at.add_column("RAW",    justify="right", min_width=16, style=FG1)
        at.add_column("ST",     justify="center", width=8)
        for a in info.smart_attrs:
            fail = a.get("failed", False)
            st_t = Text("▲ FAIL", style=f"bold {CR}") if fail else Text("● ok", style=f"bold {CG}")
            at.add_row(str(a["id"]), a["name"], str(a["value"]), str(a["worst"]),
                       str(a["thresh"]), str(a["raw"]), st_t,
                       style=f"bold {CR}" if fail else "")
        console.print(btop_panel(at, "SMART attributes"))
    else:
        console.print(btop_panel(
            Text("\n  No SMART data.  Install smartmontools and run with sudo.\n",
                 style=FG3, justify="center"),
            "SMART attributes"))
    console.print()


# ─── Background workers ────────────────────────────────────────────────────────

def _bg_refresh(state: "_State"):
    """Two-tier refresh: usage every 2s, full SMART every 30s."""
    last_smart = 0.0
    while not state.quit:
        now   = time.time()
        names = list_disk_names()
        with state.lock:
            prev_map = {d.device: d for d in state.disks}
        do_smart = (now - last_smart) >= 30.0
        fresh = []
        for n in names:
            dev  = f"/dev/{n}"
            prev = prev_map.get(dev)
            try:
                if do_smart:
                    fresh.append(fetch_disk(n, prev))
                elif prev:
                    d = DiskInfo(device=prev.device, name=prev.name)
                    for fld in ("model","serial","firmware","interface","size_bytes","size_human",
                                "health_pct","perf_pct","temp_c","smart_status","power_on_hours",
                                "reallocated_sectors","pending_sectors","uncorrectable_errors",
                                "smart_error_count","power_cycles","smart_attrs","smart_available",
                                "temp_history"):
                        setattr(d, fld, getattr(prev, fld))
                    d.used_bytes,d.free_bytes,d.total_bytes,d.used_pct = get_disk_usage(n)
                    fresh.append(d)
                else:
                    fresh.append(fetch_disk(n))
            except Exception as e:
                di = DiskInfo(device=dev, name=n); di.error_msg=str(e)
                fresh.append(di)
        if do_smart: last_smart = now
        update_io_speeds(fresh)
        with state.lock:
            existing = {d.device for d in fresh}
            state.clean_status = {k:v for k,v in state.clean_status.items() if k in existing}
            state.disks = fresh
            if state.selected >= len(fresh) and fresh:
                state.selected = max(0, len(fresh)-1)
            state._dirty = True
        state.refresh_ev.wait(timeout=2.0)
        state.refresh_ev.clear()


def _bg_clean(state: "_State", disk: DiskInfo):
    dev = disk.device
    with state.lock: state.clean_status[dev]="cleaning"; state._dirty=True
    success = False
    try:
        import contextlib
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            success = _do_fast_clean(dev)
    except Exception: success = False
    try:
        ni = fetch_disk(disk.name)
        with state.lock:
            for i,d in enumerate(state.disks):
                if d.device==dev: state.disks[i]=ni; break
    except Exception: pass
    with state.lock: state.clean_status[dev]="done" if success else "failed"; state._dirty=True
    state.refresh_ev.set()


# ─── cmd_all ──────────────────────────────────────────────────────────────────

def cmd_all():
    _warn_requirements()
    state = _State()
    with console.status(f"[{FG2}]  Scanning devices …[/{FG2}]",
                        spinner="dots", spinner_style=BOX_TITLE):
        for n in list_disk_names():
            try:   state.disks.append(fetch_disk(n))
            except Exception as e:
                di = DiskInfo(device=f"/dev/{n}",name=n); di.error_msg=str(e)
                state.disks.append(di)
    update_io_speeds(state.disks)
    threading.Thread(target=_bg_refresh, args=(state,), daemon=True).start()

    # Enter alt-screen, hide cursor, put stdin in cbreak+noecho
    _tty_setup()
    sys.stdout.write(_ALT_ON + _CUR_OFF); sys.stdout.flush()

    def _restore():
        sys.stdout.write(_ALT_OFF + _CUR_ON); sys.stdout.flush()
        _tty_restore()

    try:
        cols, rows = _term_size()
        next_frame = 0.0

        # Paint the result card exactly once, then freeze until dismissed
        painted_result_id = None

        while not state.quit:
            now = time.time()
            nc, nr = _term_size()
            resized = (nc != cols or nr != rows)
            if resized:
                cols, rows = nc, nr
                sys.stdout.write("\033[2J")
                with state.lock: state._dirty = True

            with state.lock:
                dirty          = state._dirty
                result_present = (state.iops_result is not None
                                  and not state.iops_running)
                result_id      = id(state.iops_result) if result_present else None

            # Rendering policy
            should_render = False
            if result_present:
                # Paint the result card exactly once when it first appears,
                # and repaint only on terminal resize. Background refresh
                # ticks (disk stats updating every 2 s) no longer cause
                # any repaint, so no flicker under the card.
                if painted_result_id != result_id or resized:
                    should_render = True
                    painted_result_id = result_id
            else:
                painted_result_id = None
                if dirty or now >= next_frame:
                    should_render = True

            if should_render:
                try:
                    frame = _render_frame(state, now, cols, rows)
                    # Synchronized output: terminals hold the screen buffer
                    # until the full frame is written, eliminating partial-
                    # paint flicker.
                    sys.stdout.write(_SYNC_ON + _HOME + frame + _CLR_END + _SYNC_OFF)
                    sys.stdout.flush()
                except Exception: pass
                if not result_present:
                    next_frame = now + 0.10
                with state.lock: state._dirty = False

            key = _read_key()
            if key is None: time.sleep(0.02); continue

            with state.lock:
                n_disks     = len(state.disks)
                modal       = state.modal_disk
                iops_active = state.iops_running
                iops_done   = state.iops_result is not None and not state.iops_running

            # I/O test result card: S saves, close keys dismiss
            if iops_done:
                close_keys = ("\033", "\r", "\n", " ", "q", "Q", "\x03")
                if key in ("s", "S"):
                    path = _save_iops_report(state)
                    with state.lock:
                        if state.iops_result is not None:
                            state.iops_result["saved_path"] = path
                        # Re-bind dict to force a re-render of the subtitle
                        state.iops_result = dict(state.iops_result) if state.iops_result else None
                        state._dirty = True
                elif key in close_keys:
                    with state.lock:
                        state.iops_result = None
                        state.iops_disk   = None
                        state.iops_phase  = ""
                        state._dirty = True
                continue

            # I/O test running: Ctrl-C / Esc / Q = cooperative cancel
            if iops_active:
                if key in ("\x03", "\033", "q", "Q"):
                    with state.lock:
                        state.iops_cancel = True
                        state.iops_info   = "cancelling…"
                        state._dirty = True
                continue

            # Confirm-wipe modal is up
            if modal is not None:
                if key in ("\033[D", "\033[C", "h", "l"):
                    with state.lock: state.modal_btn = 1 - state.modal_btn; state._dirty = True
                elif key in ("\r", "\n"):
                    with state.lock:
                        chosen = state.modal_btn; disk = state.modal_disk
                        state.modal_disk = None; state.modal_btn = 0; state._dirty = True
                    if chosen == 1 and disk:
                        if os.geteuid() != 0 or _is_system_disk(disk.device):
                            with state.lock: state.clean_status[disk.device] = "failed"
                        else:
                            threading.Thread(target=_bg_clean, args=(state, disk), daemon=True).start()
                elif key in ("\033", "n", "N", "\x03"):
                    with state.lock: state.modal_disk = None; state.modal_btn = 0; state._dirty = True
                elif key in ("y", "Y"):
                    with state.lock:
                        disk = state.modal_disk
                        state.modal_disk = None; state.modal_btn = 0; state._dirty = True
                    if disk and os.geteuid() == 0 and not _is_system_disk(disk.device):
                        threading.Thread(target=_bg_clean, args=(state, disk), daemon=True).start()
                # All other keys are silently swallowed — no echo.
                continue

            # Normal dashboard navigation
            if key in ("\x03", "q", "Q"):
                state.quit = True
            elif key in ("\033[A", "k"):
                with state.lock: state.selected = max(0, state.selected - 1); state._dirty = True
            elif key in ("\033[B", "j"):
                with state.lock:
                    if n_disks: state.selected = min(n_disks - 1, state.selected + 1); state._dirty = True
            elif key in ("c", "C"):
                with state.lock:
                    if 0 <= state.selected < len(state.disks):
                        state.modal_disk = state.disks[state.selected]
                        state.modal_btn  = 0
                        state._dirty     = True
            elif key in ("i", "I"):                          # I = IOPS test
                with state.lock:
                    ok = (0 <= state.selected < len(state.disks)
                          and not state.iops_running)
                    sel_disk = state.disks[state.selected] if ok else None
                if sel_disk is not None:
                    # The benchmark itself decides whether to use raw device
                    # mode (needs root) or file mode (works as any user with
                    # a writable mount on the disk), and reports a clear
                    # error from inside the worker if neither is possible.
                    threading.Thread(target=_bg_iops_test,
                                     args=(state, sel_disk), daemon=True).start()
            elif key in ("r", "R"):
                state.refresh_ev.set()
            # Anything else: silently ignored — key has been consumed already.

    except KeyboardInterrupt:
        state.quit = True
    finally:
        state.quit = True
        _restore()

    console.print(f"\n[{FG2}]  checkdisk exited.[/{FG2}]\n")




# ─── Clean commands ────────────────────────────────────────────────────────────

def _get_disk_size_bytes(device_path: str) -> int:
    rc,out,_ = run_cmd(["blockdev","--getsize64",device_path])
    if rc==0 and out.strip().isdigit(): return int(out.strip())
    rc2,out2,_ = run_cmd(["lsblk","-d","-b","-n","-o","SIZE",device_path])
    try: return int(out2.strip())
    except Exception: return 0

def _get_mounted_partitions(device_path: str) -> List[str]:
    mounts = []
    rc,out,_ = run_cmd(["lsblk","-J","-o","NAME,MOUNTPOINT",device_path])
    if rc!=0 or not out.strip(): return mounts
    try:
        def walk(n):
            mp = n.get("mountpoint")
            if mp and mp not in ("","[SWAP]",None): mounts.append(mp)
            for c in n.get("children",[]): walk(c)
        for d in json.loads(out).get("blockdevices",[]): walk(d)
    except Exception: pass
    return mounts

def _is_system_disk(device_path: str) -> bool:
    rc,out,_ = run_cmd(["lsblk","-J","-o","NAME,MOUNTPOINT,TYPE",device_path])
    if rc!=0 or not out.strip(): return False
    protected = {"/","/boot","/boot/efi","/efi","/usr","/var","/home"}
    try:
        def walk(n) -> bool:
            if (n.get("mountpoint") or "") in protected: return True
            return any(walk(c) for c in n.get("children",[]))
        for d in json.loads(out).get("blockdevices",[]):
            if walk(d): return True
    except Exception: pass
    return False

def _unmount_all(device_path: str) -> List[str]:
    errors = []
    rc,out,_ = run_cmd(["lsblk","-J","-o","NAME,TYPE,MOUNTPOINT",device_path])
    if rc==0 and out.strip():
        try:
            def do_swap(n):
                if n.get("mountpoint")=="[SWAP]": run_cmd(["swapoff",f"/dev/{n.get('name','')}"])
                for c in n.get("children",[]): do_swap(c)
            for d in json.loads(out).get("blockdevices",[]): do_swap(d)
        except Exception: pass
    for mp in reversed(_get_mounted_partitions(device_path)):
        rc2,_,err = run_cmd(["umount","-l",mp])
        if rc2!=0: errors.append(f"umount {mp}: {err.strip()}")
    return errors

def _run_with_progress(cmd: List[str], description: str) -> Tuple[int,str]:
    from rich.live import Live
    start = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    with Live(console=console, refresh_per_second=4) as live:
        while proc.poll() is None:
            elapsed = time.time()-start
            live.update(Text.assemble((f"  ⟳  {description}  {elapsed:.1f}s", FG2)))
            time.sleep(0.25)
    _,stderr = proc.communicate()
    return proc.returncode, stderr or ""

def _do_fast_clean(device_path: str) -> bool:
    console.print()
    console.print(Rule(Text(f"  UNMOUNTING  {device_path}", style=FG0), style=BOX_DIV))
    errs = _unmount_all(device_path)
    if errs: console.print(Text(f"  ⚠  Unmount issues: {'; '.join(errs)}", style=f"bold {CY}"))
    else:    console.print(Text(f"  ✓  Unmounted.", style=f"bold {CG}"))

    console.print()
    console.print(Rule(Text("  WIPING FILESYSTEM SIGNATURES  (wipefs)", style=FG0), style=BOX_DIV))
    rc,err = _run_with_progress(["wipefs","-a","--force",device_path], f"Wiping {device_path}")
    if rc!=0: console.print(Text(f"  ⚠  wipefs: {err.strip()}", style=f"bold {CY}"))
    else:     console.print(Text(f"  ✓  Signatures cleared.", style=f"bold {CG}"))

    console.print()
    console.print(Rule(Text("  DESTROYING PARTITION TABLES", style=FG0), style=BOX_DIV))
    if shutil.which("sgdisk"):
        rc2,err2 = _run_with_progress(["sgdisk","--zap-all",device_path], f"Zapping {device_path}")
        if rc2 not in (0,2): console.print(Text(f"  ⚠  sgdisk: {err2.strip()}", style=f"bold {CY}"))
    sz = _get_disk_size_bytes(device_path)
    if sz > 0:
        last = max(0,(sz//512)-4096)
        run_cmd(["dd","if=/dev/zero",f"of={device_path}","bs=512","count=4096","conv=fsync,noerror","status=none"])
        if last > 0:
            run_cmd(["dd","if=/dev/zero",f"of={device_path}","bs=512",f"seek={last}","count=4096","conv=fsync,noerror","status=none"])
    else:
        run_cmd(["dd","if=/dev/zero",f"of={device_path}","bs=1M","count=1","conv=fsync,noerror","status=none"])
    console.print(Text(f"  ✓  Partition tables destroyed.", style=f"bold {CG}"))
    run_cmd(["partprobe",device_path]); run_cmd(["blockdev","--rereadpt",device_path])
    return True

def _do_secure_erase(device_path: str) -> bool:
    _do_fast_clean(device_path)
    console.print(); sz = _get_disk_size_bytes(device_path)
    console.print(Rule(Text(f"  SECURE ERASE — ZEROING {human_size(sz) if sz else '?'}", style=f"bold {CR}"), style=CR))
    console.print(Text("  This may take a long time. Ctrl+C to abort.", style=FG3))
    console.print()
    if shutil.which("pv"):
        cmd = (f"dd if=/dev/zero bs=4M conv=fsync,noerror status=none | pv -s {sz} | dd of={device_path} bs=4M conv=fsync,noerror status=none"
               if sz else f"dd if=/dev/zero of={device_path} bs=4M conv=fsync,noerror | pv -p")
        rc = subprocess.call(cmd, shell=True)
    else:
        rc = subprocess.call(["dd","if=/dev/zero",f"of={device_path}","bs=4M","conv=fsync,noerror","status=progress"])
    if rc in (0,1):
        console.print(); console.print(Text("  ✓  Secure erase complete.", style=f"bold {CG}")); return True
    else:
        console.print(); console.print(Text(f"  ⚠  dd exited {rc} — partial erase.", style=f"bold {CY}")); return False

def cmd_clean(device_path: str, mode: str = "fast"):
    if not device_path.startswith("/dev/"): device_path = f"/dev/{device_path}"
    if not os.path.exists(device_path):
        console.print(f"\n[bold {CR}]  ✕  Not found: {device_path}[/bold {CR}]\n"); sys.exit(1)
    if os.geteuid()!=0:
        console.print(f"\n[bold {CR}]  ✕  Root required. Run: sudo checkdisk clean {device_path}[/bold {CR}]\n"); sys.exit(1)
    if _is_system_disk(device_path):
        console.print(Panel(
            Text(f"  ✕  REFUSED: {device_path} contains a system partition.\n  Wiping would destroy your OS.", style=f"bold {CR}"),
            border_style=CR, box=box.ROUNDED, padding=(0,1))); sys.exit(1)

    dev_name = device_path[5:]
    with console.status(f"[{FG2}]  Reading disk info …[/{FG2}]", spinner="dots", spinner_style=BOX_TITLE):
        try: info = fetch_disk(dev_name)
        except Exception: info = DiskInfo(device=device_path, name=dev_name)

    console.print()
    console.print(Panel(
        Text.assemble((f"  ✕  DESTRUCTIVE OPERATION\n  ALL DATA ON {device_path} WILL BE PERMANENTLY LOST", f"bold {CR}")),
        border_style=CR, box=box.ROUNDED, padding=(0,1)))
    console.print()

    det = Table(show_header=False, box=None, padding=(0,2), expand=True)
    det.add_column("k", style=FG3, min_width=14); det.add_column("v")
    det.add_row("Device",     Text(info.device,     style=f"bold {CR}"))
    det.add_row("Model",      Text(info.model,      style=FG1))
    det.add_row("Capacity",   Text(info.size_human, style=f"bold {CY}"))
    det.add_row("Operation",  Text("SECURE ERASE (zeros entire disk)" if mode=="all" else "FAST CLEAN (partition tables + signatures)", style=f"bold {CY}"))
    det.add_row("Reversible", Text("NO — data cannot be recovered",   style=f"bold {CR}"))
    if info.total_bytes>0: det.add_row("Data at risk", Text(human_size(info.used_bytes), style=FG1))
    console.print(btop_panel(det, "TARGET DISK", border=CR))
    console.print()

    # Triple confirm
    dev_name_bare = os.path.basename(device_path)
    def step(n, lbl):
        t = Text(); t.append(f"  [{n}/3] ", style=FG0); t.append(lbl, style=FG1)
        console.print(Rule(t, style=BOX_DIV))
    def prompt(p) -> str:
        console.print(Text(f"  {p}", style=FG2))
        try: return input(f"  [{FG3}❯{FG3}] ").strip()
        except (EOFError, KeyboardInterrupt): return ""

    step(1,"Acknowledge")
    if prompt("Continue?  (type  yes)").lower() not in ("yes","y"):
        console.print(f"\n[{FG3}]  Aborted.[/{FG3}]\n"); sys.exit(0)
    console.print()

    step(2,"Confirm Device")
    if prompt(f"Type exact device name to confirm:  {dev_name_bare}") != dev_name_bare:
        console.print(f"\n[bold {CR}]  ✕  Device name did not match. Aborted.[/bold {CR}]\n"); sys.exit(1)
    console.print()

    step(3,"Final Authorization")
    magic = "ERASE ALL DATA"
    if prompt(f"Type in capitals:  {magic}") != magic:
        console.print(f"\n[bold {CR}]  ✕  Phrase did not match. Aborted.[/bold {CR}]\n"); sys.exit(1)

    console.print()
    console.print(Rule(Text("  WIPE IN PROGRESS", style=f"bold {CR}"), style=CR))
    try:
        success = _do_secure_erase(device_path) if mode=="all" else _do_fast_clean(device_path)
    except KeyboardInterrupt:
        console.print(f"\n[bold {CY}]  ⚠  Interrupted — disk may be partially wiped.[/bold {CY}]\n"); sys.exit(1)

    console.print()
    if success:
        res = Table(show_header=False, box=None, padding=(0,2), expand=True)
        res.add_column("k", style=FG3, min_width=12); res.add_column("v")
        res.add_row("Device", Text(device_path,                              style=FG0))
        res.add_row("Status", Text("All partitions & signatures removed",    style=f"bold {CG}"))
        res.add_row("Next",   Text("Use fdisk/gdisk/parted to repartition",  style=FG1))
        console.print(btop_panel(res, f"✓  CLEAN COMPLETE", border=CG))
    else:
        console.print(btop_panel(
            Text("  ⚠  Clean ended with warnings.", style=f"bold {CY}"),
            "RESULT", border=CY))
    console.print()

# ─── Requirements warning ──────────────────────────────────────────────────────

def _warn_requirements():
    if not shutil.which("smartctl"):
        console.print(Text("  ⚠  smartmontools not found — sudo apt install smartmontools",
                           style=f"bold {CY}"))
    if os.geteuid()!=0:
        console.print(Text("  ⚠  Not root — SMART data limited. Try: sudo checkdisk ...",
                           style=f"bold {CY}"))

# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    if not args or "--help" in args or "-h" in args:
        console.print(HELP_TEXT); sys.exit(0)
    cmd = args[0]
    if   cmd == "all":       cmd_all()
    elif cmd == "clean":
        if len(args)<2: console.print(f"\n[bold {CR}]  Usage: checkdisk clean /dev/sdX[/bold {CR}]\n"); sys.exit(1)
        cmd_clean(args[1], mode="fast")
    elif cmd == "clean-all":
        if len(args)<2: console.print(f"\n[bold {CR}]  Usage: checkdisk clean-all /dev/sdX[/bold {CR}]\n"); sys.exit(1)
        cmd_clean(args[1], mode="all")
    elif cmd.startswith("/dev/") or (cmd and not cmd.startswith("-")):
        cmd_device(cmd)
    else:
        console.print(f"\n[bold {CR}]  Unknown command: {cmd}[/bold {CR}]\n")
        console.print(HELP_TEXT); sys.exit(1)

if __name__ == "__main__":
    main()
