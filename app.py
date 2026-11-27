import asyncio
import glob
import json
import os
from datetime import datetime, timedelta
from typing import Optional
import requests
import urllib3
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

REALTIME_API = "https://thongtinquantrac.moitruonghcm.vn/api/station-auto-logs?stationType=&shareIds=695e073e750cc3001b730f1d,695e071c750cc3001b730f01,695d1735750cc3001b730582"
CONFIG_FILE = "config.json"
HISTORY_DIR = "data_history"
NTFY_TOKEN = "tk_gej47si15da23mvx3h690ro3jkn0q"

os.makedirs(HISTORY_DIR, exist_ok=True)

DEFAULT_CONFIG = {
    "ntfy_topic": "MTDQT",
    "interval_minutes": 5,
    "stations": [],
    "thresholds": {
        "pH": {"min": 5.5, "max": 9.0, "unit": "-"},
        "COD": {"min": 0.0, "max": 75.0, "unit": "mg/L"},
        "TSS": {"min": 0.0, "max": 60.0, "unit": "mg/L"},
        "NH4": {"min": 0.0, "max": 5.0, "unit": "mg/L"},
        "Temp": {"min": 10.0, "max": 40.0, "unit": "C"},
        "Flow": {"min": 0.0, "max": 500.0, "unit": "m3/h"}
    }
}

latest_data = {
    "last_updated": "Chua co du lieu",
    "status": "Dang khoi dong...",
    "raw_sample": "",
    "stations": [],
    "dashboard": {}
}

# Bo dem RAM thong ke trong ngay (tu reset khi qua ngay moi)
daily_state = {
    "date_iso": "",     # Dinh dang YYYY-MM-DD de luu file
    "date": "",         # Dinh dang dd/mm/YYYY hien thi UI
    "seen_records": set(),
    "points": [],
    "exceed_logs": [],
    "flow_tracker": {}
}

def get_vn_now():
    return datetime.utcnow() + timedelta(hours=7)

def get_history_filepath(date_iso: str) -> str:
    safe_date = "".join(c for c in date_iso if c.isdigit() or c == "-")
    return os.path.join(HISTORY_DIR, f"history_{safe_date}.json")

def cleanup_old_history(keep_days: int = 7):
    cutoff = (get_vn_now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    for fpath in glob.glob(os.path.join(HISTORY_DIR, "history_*.json")):
        fname = os.path.basename(fpath)
        fdate = fname.replace("history_", "").replace(".json", "")
        if fdate < cutoff:
            try:
                os.remove(fpath)
            except OSError:
                pass

def save_daily_snapshot(stations_snapshot: list, dashboard_summary: dict):
    if not daily_state["date_iso"]:
        return
    fpath = get_history_filepath(daily_state["date_iso"])
    flow_serializable = {}
    for k, v in daily_state["flow_tracker"].items():
        flow_serializable[k] = {
            "name": v["name"],
            "last_dt": v["last_dt"].isoformat() if isinstance(v["last_dt"], datetime) else str(v["last_dt"]),
            "last_flow": v["last_flow"],
            "total_m3": v["total_m3"]
        }

    payload = {
        "date_iso": daily_state["date_iso"],
        "date": daily_state["date"],
        "seen_records": list(daily_state["seen_records"]),
        "points": daily_state["points"],
        "exceed_logs": daily_state["exceed_logs"],
        "flow_tracker": flow_serializable,
        "stations": stations_snapshot,
        "dashboard": dashboard_summary
    }
    try:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        cleanup_old_history(7)
    except Exception as e:
        print("Loi luu lich su ngay:", e)

def load_daily_snapshot_into_ram(date_iso: str, date_display: str):
    fpath = get_history_filepath(date_iso)
    daily_state["date_iso"] = date_iso
    daily_state["date"] = date_display
    daily_state["seen_records"] = set()
    daily_state["points"] = []
    daily_state["exceed_logs"] = []
    daily_state["flow_tracker"] = {}

    if os.path.exists(fpath):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                saved = json.load(f)
            daily_state["seen_records"] = set(saved.get("seen_records", []))
            daily_state["points"] = saved.get("points", [])
            daily_state["exceed_logs"] = saved.get("exceed_logs", [])
            ft = {}
            for k, v in saved.get("flow_tracker", {}).items():
                try:
                    dt_parsed = datetime.fromisoformat(v["last_dt"])
                except Exception:
                    dt_parsed = get_vn_now()
                ft[k] = {
                    "name": v.get("name", k),
                    "last_dt": dt_parsed,
                    "last_flow": float(v.get("last_flow", 0.0)),
                    "total_m3": float(v.get("total_m3", 0.0))
                }
            daily_state["flow_tracker"] = ft
            print(f"[HISTORY RESTORED] Da nap lai {len(daily_state['points'])} diem du lieu ngay {date_display}.")
        except Exception as e:
            print("Loi khoi phuc lich su ngay:", e)

def reset_daily_if_needed(today_iso: str, today_display: str):
    if daily_state["date_iso"] != today_iso:
        load_daily_snapshot_into_ram(today_iso, today_display)

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    save_config(DEFAULT_CONFIG)
    return DEFAULT_CONFIG

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def send_ntfy_alert(title: str, message: str, is_urgent: bool = True):
    cfg = load_config()
    topic = cfg.get("ntfy_topic", "").strip()
    if not topic:
        return
    try:
        r = requests.post(
            "https://ntfy.sh",
            json={
                "topic": topic,
                "title": title,
                "message": message,
                "priority": 5 if is_urgent else 3,
                "tags": ["rotating_light", "warning"] if is_urgent else ["white_check_mark"]
            },
            headers={
                "User-Agent": "MasteriThaoDien-Monitor/1.0",
                "Authorization": f"Bearer {NTFY_TOKEN}"
            },
            timeout=10
        )
        print(f"[NTFY SENT -> {topic}] Status={r.status_code} | Resp={r.text[:100]}")
    except Exception as e:
        print("Loi gui ntfy:", e)

def parse_metrics_from_record(record: dict, thresholds: dict):
    flat_values = {}
    logs = record.get("measuringLogs") or record.get("MeasuringLogs") or record.get("lastLog", {}).get("measuringLogs")
    if isinstance(logs, dict):
        for k, v in logs.items():
            if isinstance(v, dict) and "value" in v:
                flat_values[k.lower()] = v["value"]
            elif isinstance(v, (int, float)):
                flat_values[k.lower()] = v

    for k, v in record.items():
        if isinstance(v, (int, float)):
            flat_values[k.lower()] = v
        elif isinstance(v, dict) and "value" in v and isinstance(v["value"], (int, float)):
            flat_values[k.lower()] = v["value"]

    aliases = {
        "pH": ["ph"],
        "COD": ["cod"],
        "TSS": ["tss"],
        "NH4": ["nh4", "nh4+", "amoni", "n-nh4"],
        "Temp": ["temp", "temperature", "nhietdo", "nhiet_do"],
        "Flow": ["flow", "luuluong", "luu_luong", "q", "flowin", "flowout"]
    }

    metrics_list = []
    for param, limits in thresholds.items():
        val = None
        for alias in aliases.get(param, [param.lower()]):
            if alias in flat_values and flat_values[alias] is not None:
                try:
                    val = round(float(flat_values[alias]), 2)
                    break
                except (ValueError, TypeError):
                    pass
        if val is not None:
            is_over = val < limits["min"] or val > limits["max"]
            metrics_list.append({
                "param": param,
                "value": val,
                "unit": limits["unit"],
                "min": limits["min"],
                "max": limits["max"],
                "is_over": is_over
            })
    return metrics_list

def build_dashboard_summary(thresholds: dict):
    param_vals = {p: [] for p in thresholds.keys()}
    for pt in daily_state["points"]:
        for p, v in pt["values"].items():
            if p in param_vals:
                param_vals[p].append(v)

    stats = {}
    for p, limits in thresholds.items():
        arr = param_vals.get(p, [])
        stats[p] = {
            "avg": round(sum(arr) / len(arr), 2) if arr else None,
            "max": round(max(arr), 2) if arr else None,
            "min": round(min(arr), 2) if arr else None,
            "unit": limits["unit"],
            "limit_min": limits["min"],
            "limit_max": limits["max"]
        }

    flow_by_station = {}
    total_flow_all = 0.0
    for st_key, info in daily_state["flow_tracker"].items():
        vol = round(info.get("total_m3", 0.0), 1)
        flow_by_station[info["name"]] = vol
        total_flow_all += vol

    return {
        "date": daily_state["date"],
        "exceed_count": len(daily_state["exceed_logs"]),
        "exceed_logs": list(reversed(daily_state["exceed_logs"]))[:50],
        "total_flow_m3": round(total_flow_all, 1),
        "flow_by_station": flow_by_station,
        "stats": stats,
        "points": daily_state["points"][-900:]  # Du chua tron 24h cho ca 3 tram (288 x 3 = 864 diem)
    }

def fetch_all_stations():
    global latest_data
    cfg = load_config()
    thresholds = cfg["thresholds"]
    now_vn = get_vn_now()
    today_iso = now_vn.strftime("%Y-%m-%d")
    today_str = now_vn.strftime("%d/%m/%Y")
    reset_daily_if_needed(today_iso, today_str)

    headers = {
        "accept": "application/json",
        "referer": "https://thongtinquantrac.moitruonghcm.vn/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        res = requests.get(REALTIME_API, headers=headers, verify=False, timeout=20)
        res.raise_for_status()
        data_json = res.json()

        items = []
        if isinstance(data_json, list):
            items = data_json
        elif isinstance(data_json, dict):
            for k in ["data", "stations", "items", "result", "records"]:
                if isinstance(data_json.get(k), list):
                    items = data_json[k]
                    break

        TARGET_KEYS = {
            "kv1__cong_ty_co_phan_dau_tu_thao_dienkno_cao_tang_ct12_1",
            "kv1__cong_ty_co_phan_dau_tu_thao_dienkno_cao_tang_ct3_1",
            "kv1__cong_ty_co_phan_dau_tu_thao_dienkno_cao_tang_ct3_2"
        }

        station_results = []
        for item in items:
            st_key = str(item.get("key") or item.get("stationKey") or "").strip()
            st_name = item.get("name") or item.get("stationName") or st_key or "Tram Masteri"
            raw_name = st_name.lower()

            if st_key not in TARGET_KEYS and "thao_dien" not in st_key.lower() and "thảo điền" not in raw_name and "masteri" not in raw_name:
                continue

            log_obj = item.get("lastLog") if isinstance(item.get("lastLog"), dict) else item
            raw_time = log_obj.get("receivedAt") or log_obj.get("time") or log_obj.get("updatedAt") or item.get("receivedAt") or ""
            try:
                dt_vn = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00")) + timedelta(hours=7)
                rec_time = dt_vn.strftime("%H:%M:%S - %d/%m/%Y (Gio VN)")
                short_time = dt_vn.strftime("%H:%M")
            except Exception:
                dt_vn = now_vn
                rec_time = str(raw_time) or "Real-time"
                short_time = now_vn.strftime("%H:%M")

            metrics = parse_metrics_from_record(log_obj, thresholds)

            rec_id = f"{st_key}_{rec_time}"
            if rec_id not in daily_state["seen_records"] and metrics:
                daily_state["seen_records"].add(rec_id)
                val_map = {m["param"]: m["value"] for m in metrics}
                daily_state["points"].append({
                    "station_key": st_key,
                    "station_name": st_name,
                    "time": short_time,
                    "full_time": rec_time,
                    "values": val_map
                })

                for m in metrics:
                    if m["is_over"]:
                        daily_state["exceed_logs"].append({
                            "station": st_name,
                            "param": m["param"],
                            "value": m["value"],
                            "unit": m["unit"],
                            "limit": f"{m['min']} - {m['max']}",
                            "time": rec_time
                        })

                if "Flow" in val_map:
                    flow_val = float(val_map["Flow"])
                    tracker = daily_state["flow_tracker"].get(st_key)
                    if tracker is None:
                        daily_state["flow_tracker"][st_key] = {
                            "name": st_name,
                            "last_dt": dt_vn,
                            "last_flow": flow_val,
                            "total_m3": flow_val * (5.0 / 60.0)
                        }
                    else:
                        dt_hours = (dt_vn - tracker["last_dt"]).total_seconds() / 3600.0
                        if 0 < dt_hours <= 2.0:
                            avg_q = (tracker["last_flow"] + flow_val) / 2.0
                            tracker["total_m3"] += avg_q * dt_hours
                        tracker["last_dt"] = dt_vn
                        tracker["last_flow"] = flow_val

            station_results.append({
                "name": st_name,
                "key": st_key,
                "record_time": rec_time,
                "metrics": metrics
            })

        station_results.sort(key=lambda x: (
            1 if ("ct1" in x["name"].lower() or "ct12" in x["key"].lower()) else
            2 if ("ct3" in x["name"].lower() or "ct3_1" in x["key"].lower()) else
            3 if ("ct4" in x["name"].lower() or "ct3_2" in x["key"].lower()) else 4
        ))

        dash_summary = build_dashboard_summary(thresholds)
        save_daily_snapshot(station_results, dash_summary)

        latest_data = {
            "last_updated": now_vn.strftime("%H:%M:%S - %d/%m/%Y (Gio VN)"),
            "status": f"Ket noi Real-time thanh cong ({len(station_results)} tram)",
            "raw_sample": json.dumps(items[0] if items else data_json, ensure_ascii=False, indent=2)[:2500],
            "stations": station_results,
            "dashboard": dash_summary
        }
        print(f"[{latest_data['last_updated']}] Da cap nhat Real-time {len(station_results)} tram.")

    except Exception as e:
        latest_data["status"] = f"Loi ket noi API: {str(e)}"
        print("Loi API Real-time:", e)

async def background_scheduler():
    while True:
        await asyncio.to_thread(fetch_all_stations)
        cfg = load_config()
        await asyncio.sleep(cfg.get("interval_minutes", 5) * 60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scheduler())

@app.get("/api/data")
def get_data(date: Optional[str] = Query(default=None)):
    today_iso = get_vn_now().strftime("%Y-%m-%d")
    if not date or date == today_iso:
        return latest_data

    fpath = get_history_filepath(date)
    if not os.path.exists(fpath):
        try:
            disp = datetime.strptime(date, "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            disp = date
        return {
            "is_history": True,
            "has_data": False,
            "date_iso": date,
            "stations": latest_data.get("stations", []),
            "dashboard": {
                "date": disp,
                "exceed_count": 0,
                "exceed_logs": [],
                "total_flow_m3": 0,
                "flow_by_station": {},
                "stats": {},
                "points": []
            }
        }

    with open(fpath, "r", encoding="utf-8") as f:
        saved = json.load(f)
    return {
        "is_history": True,
        "has_data": True,
        "date_iso": date,
        "stations": saved.get("stations", []),
        "dashboard": saved.get("dashboard", {})
    }

@app.post("/api/refresh")
async def force_refresh():
    await asyncio.to_thread(fetch_all_stations)
    return latest_data

@app.get("/api/config")
def get_config():
    return load_config()

class ConfigUpdate(BaseModel):
    ntfy_topic: str
    interval_minutes: float
    stations: list = []
    thresholds: dict

@app.post("/api/config")
def update_config(new_cfg: ConfigUpdate):
    save_config(new_cfg.dict())
    return {"message": "Da luu cau hinh thanh cong!"}

@app.post("/api/test-alert")
def test_alert():
    send_ntfy_alert(
        "TEST CANH BAO - MASTERI THAO DIEN",
        "He thong Real-time 3 tram Masteri Thao Dien da san sang ban canh bao ve dien thoai!",
        is_urgent=False
    )
    return {"message": "Da gui thong bao thu nghiem toi app ntfy!"}

@app.get("/", response_class=HTMLResponse)
def serve_ui():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()
