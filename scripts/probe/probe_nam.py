import requests, re, datetime as dt
def get(u, n=600):
    try:
        r = requests.get(u, timeout=25); return r.status_code, r.text
    except Exception as e: return 0, str(e)
d = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d")
y = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y%m%d")
def ls(u):
    c, t = get(u); items = re.findall(r'href="([^"?/][^"]*)"', t)
    print(f"\n## {u} [{c}] {len(items)} entries"); print(items[:60])
    return items
ls("https://nomads.ncep.noaa.gov/pub/data/nccf/com/rrfs/prod/")
it = ls(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/rrfs/prod/rrfs.{y}/")
if it:
    sub = it[0].rstrip('/'); 
    for s in it[:3]:
        x = ls(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/rrfs/prod/rrfs.{y}/{s}")
        print([f for f in x if 'f018' in f or 'f060' in f or 'f084' in f][:20])
        print("count", len(x), "sample", x[:10], x[-10:])
for p in ["refs/prod/", "gens/prod/", "gefs_legacy/", "gefs_legacy/prod/", "hrrr/prod/", "rap/prod/", "href/prod/", "blend/prod/", "aigfs/prod/", "aigefs/prod/", "hgefs/prod/", "gfs/prod/"]:
    ls("https://nomads.ncep.noaa.gov/pub/data/nccf/com/" + p)
c, t = get(f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/refs/prod/refs.{y}/")
print("refs day", c, re.findall(r'href="([^"?/][^"]*)"', t)[:20])
c, t = get(f"https://noaa-rrfs-pds.s3.amazonaws.com/?list-type=2&prefix=rrfs_a/rrfs.{y}/00/&max-keys=60")
print("\nAWS", c, re.findall(r"<Key>([^<]+)</Key>", t)[:60])
c, t = get("https://noaa-rrfs-pds.s3.amazonaws.com/?list-type=2&delimiter=/&max-keys=50")
print("\nAWS top", re.findall(r"<Prefix>([^<]+)</Prefix>", t))
for m in ["ncep_rrfs", "ncep_hrrr_conus", "ncep_nam_conus", "ncep_rrfs_conus", "ncep_nbm_conus", "ncep_gefs025", "ncep_aigfs025", "ecmwf_aifs025_single", "ecmwf_aifs025", "gfs_graphcast025", "ncep_hgefs025_ensemble_mean"]:
    for host in ["https://api.open-meteo.com", "https://ensemble-api.open-meteo.com"]:
        c, t = get(f"{host}/data/{m}/static/meta.json")
        if c == 200: print("OM meta", host.split('//')[1].split('.')[0], m, t[:120])
