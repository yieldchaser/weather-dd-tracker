import requests, json
def get(u, p=None):
    try:
        r = requests.get(u, params=p, timeout=60)
        t = r.text
        return r.status_code, t
    except Exception as e:
        return 0, str(e)
for d in ["ncep_gefs025","ncep_gefs05","ecmwf_ifs025_ensemble","ecmwf_aifs025_ensemble","google_weathernext3_ensemble_mean","google_weathernext3","ncep_gefs_seamless"]:
    c,t=get(f"https://ensemble-api.open-meteo.com/data/{d}/static/meta.json"); print("META",d,c,t[:160])
base = {"latitude":"35.68,55.75","longitude":"139.69,37.62","daily":"temperature_2m_mean","forecast_days":16,"timezone":"UTC"}
for m in ["ecmwf_ifs025","ncep_gefs_seamless","ncep_gefs025","ecmwf_aifs025_ensemble","google_weathernext3_ensemble_mean"]:
    c,t=get("https://ensemble-api.open-meteo.com/v1/ensemble",{**base,"models":m})
    try:
        j=json.loads(t); j=j if isinstance(j,list) else [j]; d=j[0].get("daily",{})
        ks=[k for k in d if k.startswith("temperature")]; print("ENS",m,c,len(ks),"series",len(d.get("time",[])),"days", d.get(ks[0],[])[:3] if ks else t[:200])
    except Exception: print("ENS",m,c,t[:200])
# single runs API (past runs)
for host in ["https://single-runs-api.open-meteo.com/v1/ensemble","https://single-runs-api.open-meteo.com/v1/forecast","https://ensemble-api.open-meteo.com/v1/ensemble"]:
    for m in ["ecmwf_ifs025","ncep_gefs025","google_weathernext3_ensemble_mean","ecmwf_ifs025_ensemble"]:
        for run in ["2026-11-27T00:00","2026-11-26T12:00","2026-11-20T00:00"]:
            c,t=get(host,{**base,"models":m,"run":run})
            msg=t[:150]
            try:
                j=json.loads(t); j=j if isinstance(j,list) else [j]; d=j[0].get("daily",{}); ks=[k for k in d if k.startswith("temperature")]
                msg=f"{len(ks)} series, first date {d.get('time',[None])[0]}, v={d.get(ks[0],[None])[:2] if ks else None}"
            except Exception: pass
            print("RUN",host.split('/')[2],host.split('/')[-1],m,run,c,msg)
# previous-runs API
c,t=get("https://previous-runs-api.open-meteo.com/v1/forecast",{**base,"models":"ecmwf_ifs025","hourly":"temperature_2m,temperature_2m_previous_day1","daily":None,"forecast_days":3})
print("PREV",c,t[:300])
