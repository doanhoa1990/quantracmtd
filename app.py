import asyncio
import json
import os
from datetime import datetime, timedelta
import requests
import urllib3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

REALTIME_API = "https://thongtinquantrac.moitruonghcm.vn/api/station-auto-logs?stationType=&shareIds=695e073e750cc3001b730f1d,695e071c750cc3001b730f01,695d1735750cc3001b730582"
CONFIG_FILE = "config.json"
NTFY_TOKEN = "tk_gej47si15da23mvx3h690ro3jkn0q"

DEFAULT_CONFIG = {
    "ntfy_topic": "MTDQT",
    "interval_minutes": 0.25,
    "stations": [],
    "thresholds": {
        "pH": {"min": 5.5, "max": 9.0, "unit": "-"},
        "COD": {"min": 0.0, "max": 75.0, "unit": "mg/L"},
        "TSS": {"min": 0.0, "max": 70.0, "unit": "mg/L"},
        "NH4": {"min": 0.0, "max": 5.0, "unit": "mg/L"},
        "Temp": {"min": 10.0, "max": 40.0, "unit": "C"},
        "Flow": {"min": 0.0, "max": 500.0, "unit": "m3/h"}
    }
}

latest_data = {
    "last_updated": "Chua co du lieu",
    "status": "Dang khoi dong...",
    "raw_sample": "",
    "stations": []
}

sent_alerts_cache = set()

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

def fetch_all_stations():
    global latest_data, sent_alerts_cache
    cfg = load_config()
    thresholds = cfg["thresholds"]

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
            except Exception:
                rec_time = str(raw_time) or "Real-time"

            metrics = parse_metrics_from_record(log_obj, thresholds)

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

        latest_data = {
            "last_updated": (datetime.utcnow() + timedelta(hours=7)).strftime("%H:%M:%S - %d/%m/%Y (Gio VN)"),
            "status": f"Ket noi Real-time thanh cong ({len(station_results)} tram)",
            "raw_sample": json.dumps(items[0] if items else data_json, ensure_ascii=False, indent=2)[:2500],
            "stations": station_results
        }
        print(f"[{latest_data['last_updated']}] Da cap nhat Real-time {len(station_results)} tram.")

    except Exception as e:
        latest_data["status"] = f"Loi ket noi API: {str(e)}"
        print("Loi API Real-time:", e)

async def background_scheduler():
    while True:
        await asyncio.to_thread(fetch_all_stations)
        cfg = load_config()
        await asyncio.sleep(cfg.get("interval_minutes", 1) * 60)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(background_scheduler())

@app.get("/api/data")
def get_data():
    return latest_data

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
