"""Robust USGS gage-height downloader for mrms-usgs-events-2m.







Modern OGC API v1 is attempted first; NWIS IV is a station-level fallback.\n\nAll requested time windows use one contract for both services: UTC [start, end),\nwith start inclusive and end exclusive. Legacy NWIS responses are normalized to\nUTC and clipped after download so local-time request semantics cannot shift the\nperiod used by HydroEventDetector or later MRMS matching.



All distinguishable 00065 series/sensors are retained for audit. Every modern



OGC response page is persisted unchanged under downloaded_data/raw/modern;



legacy fallback responses are persisted under downloaded_data/raw/legacy.



Request audits are stored separately from raw USGS payloads. A conservative



selection creates one coherent HydroEventDetector input without silently mixing



primary/secondary sensors, sublocations, or vertical datums.



"""



from __future__ import annotations



import json, os, re



from concurrent.futures import ThreadPoolExecutor, as_completed



from dataclasses import asdict, dataclass



from pathlib import Path



from typing import Any, Sequence



import pandas as pd



import requests



from requests.adapters import HTTPAdapter



from urllib3.util.retry import Retry







PARAMETER_CODE_STAGE="00065"



MODERN_BASE_URL="https://api.waterdata.usgs.gov/ogcapi/v1"



LEGACY_IV_URL="https://waterservices.usgs.gov/nwis/iv/"



DEFAULT_TIMEOUT=120



DEFAULT_PAGE_LIMIT=50000



OBSERVATION_COLUMNS=["site_id","datetime","value","stage_source_id","qualifier","approval_status"]

STATION_COLUMNS=["site_id","agency_code","station_name","state_code","state_name","county_code","county_name","site_type_code","site_type","hydrologic_unit_code","basin_code","latitude","longitude","altitude","altitude_accuracy","altitude_method_code","altitude_method_name","vertical_datum","vertical_datum_name","drainage_area","contributing_drainage_area","time_zone","uses_daylight_savings","construction_date","source"]



TIME_SERIES_COLUMNS=["site_id","time_series_id","parameter_code","parameter_name","parameter_description","unit","statistic_id","computation_identifier","computation_period_identifier","start_datetime","end_datetime","data_gap_interval","web_description","sublocation_identifier","series_primary","vertical_datum","vertical_datum_name","parent_time_series_id","source"]

STAGE_SOURCE_COLUMNS=["site_id","stage_source_id","time_series_id","parameter_code","parameter_name","parameter_description","unit","method_category","method_id","method_description","values_block_index","sublocation_identifier","series_primary","vertical_datum","vertical_datum_name","start_datetime","end_datetime","source"]







@dataclass



class StageDownloadResult:



    state:str; site_id:str; source:str; output_directory:Path



    observations:pd.DataFrame; hydroeventdetector_input:pd.DataFrame



    excluded_observations:pd.DataFrame; station_metadata:pd.DataFrame



    time_series_metadata:pd.DataFrame; observations_path:Path



    hydroeventdetector_path:Path; excluded_observations_path:Path



    station_metadata_path:Path; time_series_metadata_path:Path; manifest_path:Path

    stage_source_metadata:pd.DataFrame|None=None

    stage_source_metadata_path:Path|None=None



@dataclass



class SourceSelection:



    status:str; selected_source_key:str|None; reason:str



    candidate_count:int; selected_rows:int; excluded_rows:int







def preserve_site_id(x:str)->str:



    x=str(x).strip()



    if not x: raise ValueError("site_id cannot be empty")



    return x



def preserve_state(x:str)->str:



    x=str(x).strip()



    if not x: raise ValueError("state cannot be empty")



    return x.upper()



def _text(x:Any)->str|None:



    if x is None:return None



    try:



        if pd.isna(x):return None



    except (TypeError,ValueError):pass



    x=str(x).strip(); return x or None



def _primary(x:Any)->bool|None:



    t=(_text(x) or "").lower()



    if t in {"primary","true","yes","y","1"}:return True



    if t in {"false","no","n","0","secondary"}:return False



    return None



def normalize_qualifier(x:Any)->str|None:



    if isinstance(x,(list,tuple,set)):



        v=[_text(i) for i in x]; v=[i for i in v if i]; return "|".join(v) if v else None



    return _text(x)



def normalize_approval_status(x:Any)->str|None:



    t=_text(x)



    return {"A":"Approved","P":"Provisional"}.get((t or "").upper(),t)



def normalize_parameter_name(c:Any,n:Any)->str|None:



    return "Gage height" if _text(c)==PARAMETER_CODE_STAGE else _text(n)



def normalize_daylight_savings(x:Any)->bool|None:



    t=(_text(x) or "").upper()



    if t in {"Y","YES","TRUE","1"}:return True



    if t in {"N","NO","FALSE","0"}:return False



    return None



def empty_observations():return pd.DataFrame(columns=OBSERVATION_COLUMNS)



def empty_station_metadata():return pd.DataFrame(columns=STATION_COLUMNS)



def empty_time_series_metadata():return pd.DataFrame(columns=TIME_SERIES_COLUMNS)

def empty_stage_source_metadata():return pd.DataFrame(columns=STAGE_SOURCE_COLUMNS)







def build_session(api_key:str|None=None)->requests.Session:



    s=requests.Session(); retry=Retry(total=6,connect=6,read=6,status=6,backoff_factor=1.0,status_forcelist=(429,500,502,503,504),allowed_methods=frozenset({"GET"}),respect_retry_after_header=True,raise_on_status=False)



    a=HTTPAdapter(max_retries=retry,pool_connections=32,pool_maxsize=32); s.mount("https://",a)



    s.headers.update({"Accept":"application/json","User-Agent":"mrms-usgs-events-2m/0.1"})



    key=api_key or os.getenv("USGS_API_KEY")



    if key:s.headers["X-Api-Key"]=key



    return s







def request_json(url:str,*,params:dict[str,Any]|None=None,session:requests.Session,timeout:int=DEFAULT_TIMEOUT):



    r=session.get(url,params=params or {},timeout=timeout); r.raise_for_status(); p=r.json()



    if not isinstance(p,dict):raise TypeError(f"Expected JSON object from {r.url}")



    return p,{"url":r.url,"x_ratelimit_limit":r.headers.get("X-RateLimit-Limit",""),"x_ratelimit_remaining":r.headers.get("X-RateLimit-Remaining","")}



def _next_link(p):



    for l in p.get("links") or []:



        if l.get("rel")=="next" and l.get("href"):return str(l["href"])



    return None



def paged_features(



    url:str,



    *,



    params:dict[str,Any],



    session:requests.Session,



    raw_directory:Path|None=None,



    raw_prefix:str|None=None,



):



    """Fetch all OGC pages, optionally persisting every untouched JSON page."""



    fs=[]; audit=[]; u=url; q=dict(params); page=0



    if raw_directory is not None:



        raw_directory.mkdir(parents=True,exist_ok=True)



    while u:



        page+=1



        payload,headers=request_json(u,params=q,session=session)



        page_features=payload.get("features") or []



        fs.extend(page_features)



        if raw_directory is not None and raw_prefix:



            write_json(raw_directory/f"{raw_prefix}_page_{page:04d}.json",payload)



        audit.append({



            "page":page,



            "number_returned":len(page_features),



            "timeStamp":payload.get("timeStamp"),



            **headers,



        })



        u=_next_link(payload); q={}



    return fs,audit



def _year_windows(start_date:str,end_date:str):



    a=pd.Timestamp(start_date); b=pd.Timestamp(end_date); a=a.tz_localize("UTC") if a.tzinfo is None else a.tz_convert("UTC"); b=b.tz_localize("UTC") if b.tzinfo is None else b.tz_convert("UTC")



    if b<=a:raise ValueError("end_date must be later than start_date")



    out=[]; c=a



    while c<b:



        e=min(pd.Timestamp(year=c.year+1,month=1,day=1,tz="UTC"),b); out.append((c,e)); c=e



    return out



def _utc_timestamp(value:Any)->pd.Timestamp:
    """Return a timezone-aware UTC timestamp."""
    ts=pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")

def clip_observations_utc(observations:pd.DataFrame,start_date:str,end_date:str)->pd.DataFrame:
    """Clip observations to canonical UTC [start, end)."""
    if observations.empty:return observations.copy()
    start=_utc_timestamp(start_date); end=_utc_timestamp(end_date)
    if end<=start:raise ValueError("end_date must be later than start_date")
    r=observations.copy(); dt=pd.to_datetime(r["datetime"],utc=True,errors="coerce")
    return r.loc[dt.notna() & (dt>=start) & (dt<end)].reset_index(drop=True)

def clip_stage_source_metadata_utc(metadata:pd.DataFrame,observations:pd.DataFrame)->pd.DataFrame:
    """Keep represented sources and refresh their UTC extents."""
    if metadata.empty or observations.empty:return metadata.iloc[0:0].copy()
    used=set(observations["stage_source_id"].dropna().astype(str))
    r=metadata[metadata["stage_source_id"].astype(str).isin(used)].copy()
    extents=observations.groupby("stage_source_id")["datetime"].agg(["min","max"]).rename(columns={"min":"start_datetime","max":"end_datetime"})
    r=r.drop(columns=["start_datetime","end_datetime"],errors="ignore").merge(extents,left_on="stage_source_id",right_index=True,how="left")
    return finalize_stage_source_metadata(r)

def finalize_observations(df:pd.DataFrame)->pd.DataFrame:

    if df.empty:return empty_observations()

    r=df.copy()

    for c in OBSERVATION_COLUMNS:

        if c not in r:r[c]=None

    r["site_id"]=r["site_id"].astype("string"); r["datetime"]=pd.to_datetime(r["datetime"],utc=True,errors="coerce"); r["value"]=pd.to_numeric(r["value"],errors="coerce"); r["stage_source_id"]=r["stage_source_id"].map(_text).astype("string"); r["qualifier"]=r["qualifier"].map(normalize_qualifier); r["approval_status"]=r["approval_status"].map(normalize_approval_status)

    r.loc[r["value"]<=-999000,"value"]=pd.NA; r=r.dropna(subset=["datetime","value","stage_source_id"])

    return r[OBSERVATION_COLUMNS].sort_values(["datetime","stage_source_id"],kind="stable").drop_duplicates(["site_id","datetime","stage_source_id"],keep="last").reset_index(drop=True)



def finalize_station_metadata(df):



    if df.empty:return empty_station_metadata()



    r=df.copy()



    for c in STATION_COLUMNS:



        if c not in r:r[c]=None



    r["site_id"]=r["site_id"].map(preserve_site_id); r["uses_daylight_savings"]=r["uses_daylight_savings"].map(normalize_daylight_savings)



    return r[STATION_COLUMNS].reset_index(drop=True)



def finalize_time_series_metadata(df):



    if df.empty:return empty_time_series_metadata()



    r=df.copy()



    for c in TIME_SERIES_COLUMNS:



        if c not in r:r[c]=None



    r["site_id"]=r["site_id"].map(preserve_site_id); r["parameter_code"]=r["parameter_code"].astype("string").str.strip(); r["parameter_name"]=[normalize_parameter_name(c,n) for c,n in zip(r["parameter_code"],r["parameter_name"])]; r["start_datetime"]=pd.to_datetime(r["start_datetime"],utc=True,errors="coerce"); r["end_datetime"]=pd.to_datetime(r["end_datetime"],utc=True,errors="coerce"); r["series_primary"]=r["series_primary"].map(_primary)



    return r[TIME_SERIES_COLUMNS].reset_index(drop=True)







def finalize_stage_source_metadata(df:pd.DataFrame)->pd.DataFrame:

    if df.empty:return empty_stage_source_metadata()

    r=df.copy()

    for c in STAGE_SOURCE_COLUMNS:

        if c not in r:r[c]=None

    r["site_id"]=r["site_id"].map(preserve_site_id); r["stage_source_id"]=r["stage_source_id"].map(_text).astype("string"); r["parameter_code"]=r["parameter_code"].astype("string").str.strip(); r["parameter_name"]=[normalize_parameter_name(c,n) for c,n in zip(r["parameter_code"],r["parameter_name"])]; r["series_primary"]=r["series_primary"].map(_primary); r["values_block_index"]=pd.to_numeric(r["values_block_index"],errors="coerce").astype("Int64"); r["start_datetime"]=pd.to_datetime(r["start_datetime"],utc=True,errors="coerce"); r["end_datetime"]=pd.to_datetime(r["end_datetime"],utc=True,errors="coerce")

    return r[STAGE_SOURCE_COLUMNS].drop_duplicates("stage_source_id",keep="last").reset_index(drop=True)



def modern_monitoring_location(site_id:str,*,session:requests.Session):



    site_id=preserve_site_id(site_id); url=f"{MODERN_BASE_URL}/collections/monitoring-locations/items"; p,h=request_json(url,params={"f":"json","id":f"USGS-{site_id}","limit":10},session=session); fs=p.get("features") or []



    if not fs:return empty_station_metadata(),{"payload":p,"request":h}



    f=fs[0]; x=f.get("properties") or {}; co=(f.get("geometry") or {}).get("coordinates") or [None,None]



    row={"site_id":site_id,"agency_code":x.get("agency_code"),"station_name":x.get("monitoring_location_name") or x.get("name"),"state_code":x.get("state_code"),"state_name":x.get("state_name"),"county_code":x.get("county_code"),"county_name":x.get("county_name"),"site_type_code":x.get("site_type_code"),"site_type":x.get("site_type"),"hydrologic_unit_code":x.get("hydrologic_unit_code"),"basin_code":x.get("basin_code"),"latitude":co[1] if len(co)>1 else None,"longitude":co[0] if co else None,"altitude":x.get("altitude"),"altitude_accuracy":x.get("altitude_accuracy"),"altitude_method_code":x.get("altitude_method_code"),"altitude_method_name":x.get("altitude_method_name"),"vertical_datum":x.get("vertical_datum"),"vertical_datum_name":x.get("vertical_datum_name"),"drainage_area":x.get("drainage_area"),"contributing_drainage_area":x.get("contributing_drainage_area"),"time_zone":x.get("time_zone_abbreviation"),"uses_daylight_savings":x.get("uses_daylight_savings"),"construction_date":x.get("construction_date"),"source":"modern_ogc_v1"}



    return finalize_station_metadata(pd.DataFrame([row])),{"payload":p,"request":h}







def modern_time_series(site_id:str,*,session:requests.Session,raw_directory:Path|None=None):



    site_id=preserve_site_id(site_id); url=f"{MODERN_BASE_URL}/collections/time-series-metadata/items"; fs,a=paged_features(url,params={"f":"json","monitoring_location_id":f"USGS-{site_id}","parameter_code":PARAMETER_CODE_STAGE,"limit":DEFAULT_PAGE_LIMIT},session=session,raw_directory=raw_directory,raw_prefix="time_series_metadata"); rows=[]



    for f in fs:



        x=f.get("properties") or {}; rows.append({"site_id":site_id,"time_series_id":x.get("id") or f.get("id"),"parameter_code":x.get("parameter_code"),"parameter_name":x.get("parameter_name") or x.get("parameter"),"parameter_description":x.get("parameter_description"),"unit":x.get("unit_of_measure"),"statistic_id":x.get("statistic_id"),"computation_identifier":x.get("computation_identifier"),"computation_period_identifier":x.get("computation_period_identifier"),"start_datetime":x.get("begin_utc") or x.get("begin") or x.get("start"),"end_datetime":x.get("end_utc") or x.get("end"),"data_gap_interval":x.get("data_gap_interval"),"web_description":x.get("web_description"),"sublocation_identifier":x.get("sublocation_identifier"),"series_primary":x.get("primary"),"vertical_datum":x.get("vertical_datum"),"vertical_datum_name":x.get("vertical_datum_name"),"parent_time_series_id":x.get("parent_time_series_id"),"source":"modern_ogc_v1"})



    return finalize_time_series_metadata(pd.DataFrame(rows)),{"pages":a,"feature_count":len(fs)}







def modern_observations(site_id:str,start_date:str,end_date:str,*,session:requests.Session,time_series_metadata:pd.DataFrame,raw_directory:Path|None=None):

    site_id=preserve_site_id(site_id); url=f"{MODERN_BASE_URL}/collections/continuous/items"; lookup={str(r.time_series_id):r._asdict() for r in time_series_metadata.itertuples(index=False) if _text(r.time_series_id)}; rows=[]; audits=[]; sources={}

    for a,b in _year_windows(start_date,end_date):

        interval=f"{a.strftime('%Y-%m-%dT%H:%M:%SZ')}/{b.strftime('%Y-%m-%dT%H:%M:%SZ')}"; year=a.strftime("%Y")

        fs,pages=paged_features(url,params={"f":"json","monitoring_location_id":f"USGS-{site_id}","parameter_code":PARAMETER_CODE_STAGE,"datetime":interval,"limit":DEFAULT_PAGE_LIMIT},session=session,raw_directory=raw_directory,raw_prefix=f"continuous_{year}")

        audits.append({"interval":interval,"pages":pages,"feature_count":len(fs)})

        for f in fs:

            x=f.get("properties") or {}; ts=_text(x.get("time_series_id") or x.get("timeseries_id")); m=lookup.get(ts,{})

            if not ts:continue

            source_id=f"modern:{ts}"

            rows.append({"site_id":site_id,"datetime":x.get("time"),"value":x.get("value"),"stage_source_id":source_id,"qualifier":normalize_qualifier(x.get("qualifier")),"approval_status":x.get("approval_status")})

            if source_id not in sources:sources[source_id]={"site_id":site_id,"stage_source_id":source_id,"time_series_id":ts,"parameter_code":x.get("parameter_code") or PARAMETER_CODE_STAGE,"parameter_name":m.get("parameter_name") or "Gage height","parameter_description":m.get("parameter_description"),"unit":x.get("unit_of_measure") or m.get("unit"),"method_category":x.get("method_category"),"method_id":None,"method_description":None,"values_block_index":None,"sublocation_identifier":m.get("sublocation_identifier"),"series_primary":m.get("series_primary"),"vertical_datum":x.get("vertical_datum") or m.get("vertical_datum"),"vertical_datum_name":x.get("vertical_datum_name") or m.get("vertical_datum_name"),"start_datetime":m.get("start_datetime"),"end_datetime":m.get("end_datetime"),"source":"modern_ogc_v1"}


    observations = finalize_observations(
        pd.DataFrame(rows)
    )

    # Enforce the same canonical time contract used by Legacy:
    # UTC [start, end), where start is inclusive and end is exclusive.
    observations = clip_observations_utc(
        observations,
        start_date,
        end_date,
    )

    stage_sources = finalize_stage_source_metadata(
        pd.DataFrame(sources.values())
    )

    # Remove source metadata that no longer has observations after
    # UTC clipping and update source start/end times from the
    # observations actually retained.
    stage_sources = clip_stage_source_metadata_utc(
        stage_sources,
        observations,
    )

    return observations, stage_sources, audits




def _legacy_parse(payload:dict[str,Any],site_id:str):

    obs=[]; series=[]; sources=[]; station=None

    for si,sr in enumerate(payload.get("value",{}).get("timeSeries",[]) or []):

        src=sr.get("sourceInfo") or {}; var=sr.get("variable") or {}; codes=src.get("siteCode") or []; sid=preserve_site_id(codes[0].get("value") if codes else site_id); vc=var.get("variableCode") or []; pc=vc[0].get("value") if vc else PARAMETER_CODE_STAGE; unit=(var.get("unit") or {}).get("unitCode") or (var.get("unit") or {}).get("unitAbbreviation"); ts=_text(sr.get("name")) or f"legacy_{sid}_{si}"

        if station is None:

            geo=((src.get("geoLocation") or {}).get("geogLocation") or {}); props={i.get("name"):i.get("value") for i in src.get("siteProperty") or []}; tz=src.get("timeZoneInfo") or {}; dtz=tz.get("defaultTimeZone") or {}; station={"site_id":sid,"agency_code":codes[0].get("agencyCode") if codes else "USGS","station_name":src.get("siteName"),"state_code":props.get("stateCd"),"state_name":None,"county_code":props.get("countyCd"),"county_name":None,"site_type_code":props.get("siteTypeCd"),"site_type":None,"hydrologic_unit_code":props.get("hucCd"),"basin_code":None,"latitude":geo.get("latitude"),"longitude":geo.get("longitude"),"altitude":None,"altitude_accuracy":None,"altitude_method_code":None,"altitude_method_name":None,"vertical_datum":None,"vertical_datum_name":None,"drainage_area":None,"contributing_drainage_area":None,"time_zone":dtz.get("zoneAbbreviation") or dtz.get("zoneOffset"),"uses_daylight_savings":tz.get("siteUsesDaylightSavingsTime"),"construction_date":None,"source":"legacy_nwis_iv"}

        series.append({"site_id":sid,"time_series_id":ts,"parameter_code":pc,"parameter_name":var.get("variableName"),"parameter_description":var.get("variableDescription"),"unit":unit,"statistic_id":None,"computation_identifier":"Instantaneous","computation_period_identifier":"Points","start_datetime":None,"end_datetime":None,"data_gap_interval":None,"web_description":None,"sublocation_identifier":None,"series_primary":None,"vertical_datum":None,"vertical_datum_name":None,"parent_time_series_id":None,"source":"legacy_nwis_iv"})

        for bi,block in enumerate(sr.get("values") or []):

            methods=block.get("method") or []; mids="|".join(str(m.get("methodID")).strip() for m in methods if m.get("methodID") is not None) or None; mdesc="|".join(str(m.get("methodDescription")).strip() for m in methods if _text(m.get("methodDescription"))) or None; source_id=f"legacy:{ts}:block={bi}:method={mids or ''}"; vals=block.get("value") or []; times=[o.get("dateTime") for o in vals if o.get("dateTime")]

            sources.append({"site_id":sid,"stage_source_id":source_id,"time_series_id":ts,"parameter_code":pc,"parameter_name":var.get("variableName"),"parameter_description":var.get("variableDescription"),"unit":unit,"method_category":None,"method_id":mids,"method_description":mdesc,"values_block_index":bi,"sublocation_identifier":None,"series_primary":None,"vertical_datum":None,"vertical_datum_name":None,"start_datetime":min(times) if times else None,"end_datetime":max(times) if times else None,"source":"legacy_nwis_iv"})

            for o in vals:

                qs=o.get("qualifiers") or []; approval=None; other=[]

                for q in qs:

                    q=str(q).strip()

                    if q in {"A","P"}:approval=q

                    elif q:other.append(q)

                obs.append({"site_id":sid,"datetime":o.get("dateTime"),"value":o.get("value"),"stage_source_id":source_id,"qualifier":normalize_qualifier(other),"approval_status":approval})

    return finalize_observations(pd.DataFrame(obs)),finalize_station_metadata(pd.DataFrame([station]) if station else pd.DataFrame()),finalize_time_series_metadata(pd.DataFrame(series)),finalize_stage_source_metadata(pd.DataFrame(sources))



def legacy_iv(site_id:str,start_date:str,end_date:str,*,session:requests.Session):
    """Download Legacy NWIS IV and enforce canonical UTC [start, end)."""
    start=_utc_timestamp(start_date); end=_utc_timestamp(end_date)
    if end<=start:raise ValueError("end_date must be later than start_date")
    request_start=(start-pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    request_end=(end+pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    p,h=request_json(LEGACY_IV_URL,params={"format":"json","sites":preserve_site_id(site_id),"parameterCd":PARAMETER_CODE_STAGE,"startDT":request_start,"endDT":request_end,"siteStatus":"all"},session=session)
    o,s,t,sm=_legacy_parse(p,site_id)
    downloaded_rows=len(o)
    o=clip_observations_utc(o,start_date,end_date)
    sm=clip_stage_source_metadata_utc(sm,o)
    audit={"request":h,"requested_utc_start":start.isoformat(),"requested_utc_end_exclusive":end.isoformat(),"legacy_request_start_local_calendar":request_start,"legacy_request_end_local_calendar":request_end,"downloaded_rows_before_utc_clip":downloaded_rows,"rows_after_utc_clip":len(o),"utc_window_semantics":"[start, end)"}
    return o,s,t,sm,p,audit



def _bad_location(text: str) -> bool:

    """Identify a stage source representing an alternate physical location."""



    return bool(

        re.search(

            r"\b("

            r"upstream|downstream|"

            r"headwater|tailwater|"

            r"grade control|gcs|"

            r"upper stage|lower stage|"

            r"above dam|below dam|"

            r"upstream of dam|downstream of dam|"

            r"upstream of weir|downstream of weir"

            r")\b",

            text,

            re.I,

        )

    )





def _secondary(text: str) -> bool:

    """Identify secondary, backup, or auxiliary measurements."""



    return bool(

        re.search(

            r"\b("

            r"secondary|"

            r"backup|back-up|"

            r"alternate|auxiliary|"

            r"backup sensor|secondary sensor"

            r")\b",

            text,

            re.I,

        )

    )





def _primary_hint(text: str) -> bool:

    """Identify descriptions explicitly indicating the preferred sensor."""



    return bool(

        re.search(

            r"\b("

            r"primary|"

            r"primary sensor|"

            r"main sensor|"

            r"stilling well"

            r")\b",

            text,

            re.I,

        )

    )





def select_stage_source(

    observations: pd.DataFrame,

    stage_source_metadata: pd.DataFrame,

):

    """

    Select one coherent gage-height source for HydroEventDetector.



    Metadata are evaluated at source level. The selection deliberately

    penalizes alternate physical locations and secondary/backup sensors

    before observation count is considered.

    """



    if observations.empty:

        return (

            observations.copy(),

            observations.copy(),

            SourceSelection(

                "no_data",

                None,

                "no observations",

                0,

                0,

                0,

            ),

        )



    counts = (

        observations

        .groupby("stage_source_id")

        .size()

        .to_dict()

    )



    candidates = []



    for _, metadata in stage_source_metadata.iterrows():



        key = _text(metadata.get("stage_source_id"))



        if not key or key not in counts:

            continue



        method_description = (

            _text(metadata.get("method_description")) or ""

        )



        method_category = (

            _text(metadata.get("method_category")) or ""

        )



        sublocation = (

            _text(metadata.get("sublocation_identifier")) or ""

        )



        # Important:

        # Legacy often stores Upstream/Downstream in method_description,

        # while Modern normally exposes it through sublocation_identifier.

        # Evaluate all descriptive metadata together.

        descriptive_text = " ".join(

            part

            for part in [

                method_description,

                method_category,

                sublocation,

            ]

            if part

        )



        score = 0

        reasons = []



        # Modern metadata may explicitly identify the series as primary.

        if metadata.get("series_primary") is True:

            score += 100

            reasons.append("modern_primary")



        # A source without a named sublocation gets a modest preference,

        # but only when it is not identified elsewhere as an alternate

        # physical location.

        if not sublocation:

            score += 20

            reasons.append("no_sublocation")



        # This MUST inspect the combined descriptive text, not only

        # sublocation_identifier. Legacy commonly places "Upstream",

        # "Tailwater", etc. in method_description.

        if _bad_location(descriptive_text):

            score -= 200

            reasons.append("alternate_physical_location")



        if _primary_hint(descriptive_text):

            score += 75

            reasons.append("primary_method_hint")



        if _secondary(descriptive_text):

            score -= 150

            reasons.append("secondary_or_backup")



        candidates.append(

            {

                "key": key,

                "score": score,

                "rows": int(counts[key]),

                "reasons": reasons,

            }

        )



    if not candidates:

        excluded = observations.copy()

        excluded["exclusion_reason"] = (

            "no_stage_source_candidate"

        )



        return (

            observations.iloc[0:0].copy(),

            excluded,

            SourceSelection(

                "ambiguous",

                None,

                "no source candidate",

                0,

                0,

                len(excluded),

            ),

        )



    candidates_df = (

        pd.DataFrame(candidates)

        .sort_values(

            ["score", "rows"],

            ascending=[False, False],

        )

        .reset_index(drop=True)

    )



    top = candidates_df.iloc[0]



    ties = candidates_df[

        candidates_df["score"] == top["score"]

    ]



    # Observation count must not decide between two genuinely different

    # equally preferred sources. If equally ranked sources exist, compare

    # their overlapping observations.

    if len(ties) > 1:



        first_key = ties.iloc[0]["key"]



        base = (

            observations[

                observations["stage_source_id"] == first_key

            ]

            .set_index("datetime")["value"]

        )



        identical = True



        for key in ties["key"].iloc[1:]:



            other = (

                observations[

                    observations["stage_source_id"] == key

                ]

                .set_index("datetime")["value"]

            )



            common = base.index.intersection(other.index)



            if len(common) == 0:

                identical = False

                break



            if not base.loc[common].equals(

                other.loc[common]

            ):

                identical = False

                break



        if not identical:

            excluded = observations.copy()

            excluded["exclusion_reason"] = (

                "ambiguous_stage_source"

            )



            return (

                observations.iloc[0:0].copy(),

                excluded,

                SourceSelection(

                    "ambiguous",

                    None,

                    (

                        "multiple equally ranked "

                        "non-identical stage sources"

                    ),

                    len(candidates_df),

                    0,

                    len(excluded),

                ),

            )



    selected_key = str(top["key"])



    selected = observations[

        observations["stage_source_id"] == selected_key

    ].copy()



    excluded = observations[

        observations["stage_source_id"] != selected_key

    ].copy()



    excluded["exclusion_reason"] = (

        "non_selected_stage_source"

    )



    selected = (

        selected

        .sort_values("datetime")

        .drop_duplicates(

            subset="datetime",

            keep="last",

        )

        .reset_index(drop=True)

    )



    return (

        selected,

        excluded,

        SourceSelection(

            "selected",

            selected_key,

            ";".join(top["reasons"]),

            len(candidates_df),

            len(selected),

            len(excluded),

        ),

    )





def prepare_hydroeventdetector_input(observations:pd.DataFrame,stage_source_metadata:pd.DataFrame|None=None):

    selected,excluded,decision=select_stage_source(observations,stage_source_metadata if stage_source_metadata is not None else empty_stage_source_metadata()); hydro=pd.DataFrame({"datetime":pd.to_datetime(selected["datetime"],utc=True).dt.tz_localize(None).astype("datetime64[ns]"),"Stage_ft":pd.to_numeric(selected["value"]).astype("float64")}); excluded=excluded.reindex(columns=[*OBSERVATION_COLUMNS,"exclusion_reason"]); return hydro,excluded,decision



def write_json(path:Path,payload:Any):



    path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(payload,indent=2,ensure_ascii=False,default=str),encoding="utf-8")



def download_stage_data(state:str,site_id:str,start_date:str,end_date:str,*,output_root:str|Path="data",api_key:str|None=None,session:requests.Session|None=None,save_raw_observations:bool=False,save_raw_metadata:bool=True)->StageDownloadResult:

    state=preserve_state(state); site_id=preserve_site_id(site_id); own=session is None; session=session or build_session(api_key); d=Path(output_root)/state/site_id/"downloaded_data"; d.mkdir(parents=True,exist_ok=True)

    raw_root=d/"raw"; modern_raw=raw_root/"modern"; legacy_raw_dir=raw_root/"legacy"

    modern_errors=[]; modern_station=empty_station_metadata(); modern_series=empty_time_series_metadata(); modern_sources=empty_stage_source_metadata(); modern_data=empty_observations(); station_raw={}; series_audit={}; observations_audit=[]

    try:

        modern_station,station_raw=modern_monitoring_location(site_id,session=session)

        if save_raw_metadata and station_raw.get("payload"):write_json(modern_raw/"monitoring_location.json",station_raw["payload"])

        modern_series,series_audit=modern_time_series(site_id,session=session,raw_directory=(modern_raw/"time_series_metadata") if save_raw_metadata else None)

        modern_data,modern_sources,observations_audit=modern_observations(site_id,start_date,end_date,session=session,time_series_metadata=modern_series,raw_directory=(modern_raw/"continuous") if save_raw_observations else None)

    except Exception as e:modern_errors.append(f"{type(e).__name__}: {e}")

    if not modern_data.empty:

        source="modern_ogc_v1"; observations=modern_data; stage_sources=modern_sources; station_metadata=modern_station; time_series_metadata=modern_series; fallback=False; fallback_reason=None

    else:

        fallback=True; fallback_reason="modern_error" if modern_errors else "modern_no_data"

        try:observations,legacy_station,legacy_series,stage_sources,legacy_raw,legacy_audit=legacy_iv(site_id,start_date,end_date,session=session)

        except Exception as e:

            if own:session.close()

            raise RuntimeError(f"Modern unavailable ({modern_errors or ['no data']}); legacy also failed: {e}") from e

        source="legacy_nwis_iv"; station_metadata=modern_station if not modern_station.empty else legacy_station; time_series_metadata=modern_series if not modern_series.empty else legacy_series

        if save_raw_observations:legacy_raw_dir.mkdir(parents=True,exist_ok=True);write_json(legacy_raw_dir/"iv_response.json",legacy_raw)

        write_json(d/"legacy_request_audit.json",legacy_audit)

    write_json(d/"modern_monitoring_location_request_audit.json",station_raw.get("request",{})); write_json(d/"modern_time_series_metadata_request_audit.json",series_audit); write_json(d/"modern_observations_request_audit.json",observations_audit); write_json(d/"modern_errors.json",modern_errors)

    hydro,excluded,decision=prepare_hydroeventdetector_input(observations,stage_sources)

    op=d/"stage_observations.parquet"; hp=d/"stage_hydroeventdetector.parquet"; ep=d/"excluded_observations.parquet"; sp=d/"station_metadata.parquet"; tp=d/"time_series_metadata.parquet"; smp=d/"stage_source_metadata.parquet"; mp=d/"download_manifest.json"; selp=d/"stage_source_selection.json"

    observations.to_parquet(op,index=False); hydro.to_parquet(hp,index=False); excluded.to_parquet(ep,index=False); station_metadata.to_parquet(sp,index=False); time_series_metadata.to_parquet(tp,index=False); stage_sources.to_parquet(smp,index=False); write_json(selp,asdict(decision))

    manifest={"schema_version":5,"time_standard":"UTC","time_window_semantics":"[start, end)","state":state,"site_id":site_id,"parameter_code":PARAMETER_CODE_STAGE,"requested_start":str(start_date),"requested_end":str(end_date),"selected_source":source,"fallback_attempted":fallback,"fallback_reason":fallback_reason,"modern_errors":modern_errors,"save_raw_metadata":save_raw_metadata,"save_raw_observations":save_raw_observations,"observation_rows":len(observations),"stage_source_rows":len(stage_sources),"hydroeventdetector_rows":len(hydro),"excluded_observation_rows":len(excluded),"source_selection":asdict(decision),"observation_columns":OBSERVATION_COLUMNS,"stage_source_metadata_columns":STAGE_SOURCE_COLUMNS,"station_metadata_columns":STATION_COLUMNS,"time_series_metadata_columns":TIME_SERIES_COLUMNS,"files":{"observations":op.name,"hydroeventdetector_input":hp.name,"excluded_observations":ep.name,"station_metadata":sp.name,"time_series_metadata":tp.name,"stage_source_metadata":smp.name,"source_selection":selp.name}}

    write_json(mp,manifest)

    if own:session.close()

    if observations.empty:raise RuntimeError(f"No stage observations found for {site_id} between {start_date} and {end_date}")

    return StageDownloadResult(state,site_id,source,d,observations,hydro,excluded,station_metadata,time_series_metadata,op,hp,ep,sp,tp,mp,stage_sources,smp)



def download_many_stations(stations:Sequence[tuple[str,str]],start_date:str,end_date:str,*,output_root:str|Path="data",api_key:str|None=None,max_workers:int=6)->pd.DataFrame:



    """Download many (state, site_id) pairs concurrently and return a status table."""



    def one(item):



        state,site=item



        try:



            r=download_stage_data(state,site,start_date,end_date,output_root=output_root,api_key=api_key)



            return {"state":state,"site_id":site,"status":"ok","source":r.source,"observations":len(r.observations),"hydroeventdetector_rows":len(r.hydroeventdetector_input),"error":None}



        except Exception as e:return {"state":state,"site_id":site,"status":"error","source":None,"observations":0,"hydroeventdetector_rows":0,"error":f"{type(e).__name__}: {e}"}



    rows=[]



    with ThreadPoolExecutor(max_workers=max_workers) as pool:



        futures={pool.submit(one,x):x for x in stations}



        for f in as_completed(futures):rows.append(f.result())



    return pd.DataFrame(rows).sort_values(["state","site_id"]).reset_index(drop=True)