import re, requests, datetime as dt, os
from ecmwf.opendata import Client
c = Client(source="ecmwf")
for model, typ in [("ifs", "em"), ("ifs", "es"), ("aifs-ens", "em"), ("aifs-ens", "cf"), ("aifs-ens", "pf")]:
    try:
        kw = dict(model=model, stream="enfo", type=typ, param="2t", step=24, target=f"/tmp/{model}_{typ}.grib2")
        if typ == "pf": kw["number"] = 1
        r = c.retrieve(**kw)
        print("ECMWF", model, typ, "OK", r.datetime, os.path.getsize(kw["target"]))
    except Exception as e:
        print("ECMWF", model, typ, "FAIL", str(e)[:160])
def ls(u):
    try:
        t = requests.get(u, timeout=30).text; return re.findall(r'href="([^"?/][^"]*)"', t)
    except Exception as e: return [str(e)]
d = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
y = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y%m%d")
x = ls(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigefs/prod/aigefs.{y}/")
print("AIGEFS day", x[:10])
if x:
    x2 = ls(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigefs/prod/aigefs.{y}/{x[0]}")
    print("AIGEFS cyc", x2[:20])
    for s in x2[:3]:
        x3 = ls(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/aigefs/prod/aigefs.{y}/{x[0]}{s}")
        print(" ", s, len(x3), x3[:12], [f for f in x3 if 'avg' in f or 'mean' in f][:6])
for u in ["https://dd.weather.gc.ca/ensemble/geps/grib2/products/00/024/", "https://dd.weather.gc.ca/ensemble/geps/grib2/raw/00/024/", f"https://dd.weather.gc.ca/{y}/WXO-DD/ensemble/geps/grib2/products/00/024/"]:
    z = ls(u); print("GEPS", u, len(z), [f for f in z if 'TMP' in f and 'TGL_2' in f][:8])
