import requests, datetime as dt
d = dt.datetime.now(dt.timezone.utc)
days = [(d - dt.timedelta(days=k)).strftime("%Y%m%d") for k in (0, 1, 3, 10, 26)]
for day in days:
    for name, url in [
        ("NOMADS nam", f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.{day}/nam.t00z.awphys18.tm00.grib2.idx"),
        ("NOMADS nam dir", f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/nam/prod/nam.{day}/"),
        ("AWS nam", f"https://noaa-nam-pds.s3.amazonaws.com/nam.{day}/nam.t00z.awphys18.tm00.grib2.idx"),
        ("NOMADS rrfs dir", f"https://nomads.ncep.noaa.gov/pub/data/nccf/com/rrfs/prod/rrfs.{day}/"),
        ("AWS rrfs", f"https://noaa-rrfs-pds.s3.amazonaws.com/?list-type=2&prefix=rrfs_a/rrfs.{day}/00/&max-keys=3"),
    ]:
        try:
            r = requests.get(url, timeout=20)
            print(day, name, r.status_code, r.text[:140].replace("\n", " "))
        except Exception as e:
            print(day, name, "ERR", str(e)[:100])
r = requests.get("https://nomads.ncep.noaa.gov/pub/data/nccf/com/", timeout=20)
import re
print("nomads com dirs:", sorted(set(re.findall(r'href="([a-z0-9_]+)/"', r.text)))[:80])
