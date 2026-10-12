"""Conservative NOAA–USGS Action Stage reference audit. No source data is modified.
Run: PYTHONPATH="$PWD/mrms_usgs_events_2m" python -m mrms_usgs_events_2m.action_stage_datum_correction --limit 20
"""
import argparse
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path.home()/'.tethys/Tethys_flood_2m/media/usgs_mrms_2m/app/stage_data'
BASE = Path.home()/'Documents/usgs_mrms_2m'

def num(x):
    try:
        v=float(x)
        return v if math.isfinite(v) and v not in (-999,-9999) else None
    except (ValueError,TypeError): return None

def inventory(path):
    d=pd.read_csv(path,sep='\t',dtype=str)
    required={'site_id','state','supplied_NWS_LID','Action_Stage_ft_exact_USGS_ID'}
    if not required.issubset(d): raise ValueError(f'Missing columns: {required-set(d)}')
    d['site_id']=d.site_id.str.zfill(8)
    d['action_stage_original_ft']=pd.to_numeric(d.Action_Stage_ft_exact_USGS_ID,errors='coerce')
    d=d[d.action_stage_original_ft.notna()].copy()
    if d.site_id.duplicated().any(): raise ValueError('Duplicate site_id')
    return d

def noaa(session,lid,cache):
    if cache.exists() and time.time()-cache.stat().st_mtime < 12*3600: return json.loads(cache.read_text())
    r=session.get(f'https://api.water.noaa.gov/nwps/v1/gauges/{lid}',timeout=45)
    r.raise_for_status(); data=r.json()
    cache.parent.mkdir(parents=True,exist_ok=True)
    cache.write_text(json.dumps(data,indent=2))
    return data

def selected_modern_candidates(session, site, when, folder):
    selection = json.loads((folder/'downloaded_data/stage_source_selection.json').read_text())
    if selection.get('status') != 'selected' or int(selection.get('candidate_count',1)) != 1:
        raise ValueError('source_not_uniquely_selected')
    key = selection.get('selected_source_key') or ''
    if not key.startswith('modern:'):
        raise ValueError('selected_source_not_modern')
    ts = key.split(':',1)[1]
    meta = pd.read_parquet(folder/'downloaded_data/stage_source_metadata.parquet')
    matched = meta[meta.stage_source_id.astype(str)==key]
    if len(matched)!=1 or str(matched.iloc[0].time_series_id)!=ts:
        raise ValueError('selected_source_metadata_mismatch')
    start=(when-pd.Timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    end=(when+pd.Timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    url='https://api.waterdata.usgs.gov/ogcapi/v1/collections/continuous/items'
    params={'f':'json','monitoring_location_id':f'USGS-{site}',
            'parameter_code':'00065','datetime':f'{start}/{end}','limit':1000}
    out=[]; pages=0
    while url:
        pages+=1
        if pages>20: raise ValueError('too_many_pages')
        r=session.get(url,params=params,timeout=90);r.raise_for_status()
        payload=r.json()
        for feature in payload.get('features') or []:
            x=feature.get('properties') or {}
            if str(x.get('time_series_id') or x.get('timeseries_id') or '')!=ts: continue
            v=num(x.get('value'))
            unit=x.get('unit_of_measure')
            if unit and str(unit).lower() not in ('ft','feet','foot'):
                raise ValueError('unexpected_usgs_unit')
            if v is None: continue
            t=pd.to_datetime(x.get('time'),utc=True,errors='coerce')
            if pd.notna(t):out.append((t,v))
        url=next((link.get('href') for link in payload.get('links',[])
                  if link.get('rel')=='next' and link.get('href')),None)
        params=None
    return out

def historical_offsets(data,folder):
    p=folder/'downloaded_data/stage_hydroeventdetector.parquet'
    if not p.exists(): return []
    d=pd.read_parquet(p,columns=['datetime','Stage_ft'])
    d.datetime=pd.to_datetime(d.datetime,utc=True,errors='coerce')
    d.Stage_ft=pd.to_numeric(d.Stage_ft,errors='coerce')
    d=d.dropna(subset=['datetime','Stage_ft'])
    crests=(data.get('flood') or {}).get('crests') or {}
    seen=set(); offsets=[]
    for group in ('historic','recent'):
        for c in crests.get(group) or []:
            raw=c.get('occurredTime'); v=num(c.get('stage'))
            if raw is None or v is None or c.get('olddatum') is True: continue
            try: dt=pd.to_datetime(raw,utc=True)
            except (ValueError,TypeError): continue
            key=(str(dt),v)
            if key in seen: continue
            seen.add(key)
            day=dt.normalize()
            part=d[(d.datetime>=day)&(d.datetime<day+pd.Timedelta(days=1))]
            if not part.empty: offsets.append(v-float(part.Stage_ft.max()))
    return offsets

def check(row,root,cache,noaa_session,usgs_session):
    site=row['site_id']
    raw_lid=row['supplied_NWS_LID']
    lid='' if pd.isna(raw_lid) else str(raw_lid).strip().upper()
    folder=root/row['state']/site
    result={'site_id':site,'validation_status':'pending','reason':'',
            'action_stage_corrected_ft':None,'noaa_minus_usgs_ft':None,
            'historical_matches':0,'historical_offset_range_ft':None,
            'noaa_observed_utc':None,'usgs_observed_utc':None,
            'noaa_datum':None,'noaa_datum_value_ft':None,
            'checked_at_utc':datetime.now(timezone.utc).isoformat()}
    try: selection=json.loads((folder/'downloaded_data/stage_source_selection.json').read_text())
    except (OSError,ValueError):
        result['reason']='missing_source_selection';return result
    if selection.get('status')!='selected':
        result['reason']='source_not_selected';return result
    if int(selection.get('candidate_count',1))!=1:
        result['reason']='multiple_sources_manual_review';return result
    if lid in ('','NAN','NONE'):
        result['reason']='missing_nws_lid';return result
    data=noaa(noaa_session,lid,cache/f'{lid}.json')
    if str(data.get('usgsId','')).zfill(8)!=site:
        result['reason']='noaa_usgs_id_mismatch';return result
    datums=((data.get('datums') or {}).get('vertical') or {}).get('value') or []
    if datums:
        result['noaa_datum']=datums[0].get('abbrev')
        result['noaa_datum_value_ft']=num(datums[0].get('value'))
    obs=((data.get('status') or {}).get('observed') or {})
    noaa_stage=num(obs.get('primary'))
    if noaa_stage is None or obs.get('primaryUnit')!='ft' or not obs.get('validTime'):
        result['reason']='no_current_noaa_observation';return result
    when=pd.to_datetime(obs['validTime'],utc=True)
    result['noaa_observed_utc']=when.isoformat()
    if abs((pd.Timestamp.now(tz='UTC')-when).total_seconds())>48*3600:
        result['reason']='stale_noaa_observation';return result
    candidates=selected_modern_candidates(usgs_session,site,when,folder)
    if not candidates:
        result['reason']='no_usgs_observation';return result
    seconds=min(abs((t-when).total_seconds()) for t,v in candidates)
    nearest=[(t,v) for t,v in candidates if abs(abs((t-when).total_seconds())-seconds)<.01]
    if seconds>15*60:
        result['reason']='timestamp_difference_over_15min';return result
    if max(v for t,v in nearest)-min(v for t,v in nearest)>.01:
        result['reason']='conflicting_usgs_sources';return result
    t,usgs_stage=nearest[0]
    result['usgs_observed_utc']=t.isoformat()
    offset=round(noaa_stage-usgs_stage,4)
    result['noaa_minus_usgs_ft']=offset
    historical=historical_offsets(data,folder)
    consistent=[x for x in historical if abs(x-offset)<=.1]
    result['historical_total']=len(historical)
    result['historical_consistency_fraction']=round(len(consistent)/len(historical),4) if historical else None
    result['historical_matches']=len(consistent)
    if consistent: result['historical_offset_range_ft']=round(max(consistent)-min(consistent),4)
    original=float(row['action_stage_original_ft'])
    result['reference_compatible']=abs(offset)<=.10
    result['correction_applied']=False
    if result['reference_compatible']:
        result['action_stage_corrected_ft']=original
        result['validation_status']='reference_compatible'
        result['reason']='selected_modern_source;offset_within_0.10_ft'
    elif len(consistent)>=2 and len(consistent)/len(historical)>=.80:
        result['action_stage_corrected_ft']=round(original-offset,4)
        result['correction_applied']=True
        result['validation_status']='historically_supported'
        result['reason']='selected_modern_source;daily_crest_consistency'
    else:
        result['action_stage_corrected_ft']=None
        result['action_stage_candidate_ft']=round(original-offset,4)
        result['validation_status']='provisional'
        result['reason']='large_offset_requires_validation'
    return result

def save_csv(df,records,path):
    result=df.copy()
    extra=pd.DataFrame(records.values())
    if not extra.empty: result=result.merge(extra,on='site_id',how='left',validate='one_to_one')
    if 'validation_status' not in result: result['validation_status']='pending'
    result['validation_status']=result.validation_status.fillna('pending')
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix('.tmp');result.to_csv(temp,index=False);temp.replace(path)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inventory',type=Path,default=BASE/'us_basins_action_stage_nwps.txt')
    p.add_argument('--root',type=Path,default=ROOT)
    p.add_argument('--output',type=Path,default=BASE/'us_basins_action_stage_corrected.csv')
    p.add_argument('--limit',type=int,default=20)
    p.add_argument('--all',action='store_true')
    p.add_argument('--dry-run',action='store_true')
    p.add_argument('--sleep',type=float,default=1.0)
    p.add_argument('--recheck',action='store_true')
    args=p.parse_args()
    df=inventory(args.inventory)
    run=args.root/'_action_stage_datum_run';checkpoint=run/'audit_records.json'
    records=json.loads(checkpoint.read_text()) if checkpoint.exists() else {}
    pending=[r for r in df.to_dict('records') if (r['site_id'] in records if args.recheck else r['site_id'] not in records)]
    scheduled=pending if args.all else pending[:max(args.limit,0)]
    print(f'Inventory={len(df)} already_audited={len(records)} scheduled={len(scheduled)} dry_run={args.dry_run}',flush=True)
    if args.dry_run:
        for r in scheduled:print('[WOULD AUDIT]',r['state'],r['site_id'],r['supplied_NWS_LID'])
        return
    if args.recheck and checkpoint.exists():
        backup=run/'audit_records.before_modern_recheck.json'
        if not backup.exists(): backup.write_bytes(checkpoint.read_bytes())
    from .stage_download import build_session
    usgs_session=build_session()
    noaa_session=requests.Session()
    noaa_session.headers.update({'Accept':'application/json'})
    for i,r in enumerate(scheduled,1):
        try: result=check(r,args.root,run/'noaa_cache',noaa_session,usgs_session)
        except (requests.RequestException,ValueError,KeyError,OSError) as e:
            result={'site_id':r['site_id'],'validation_status':'error','reason':f'{type(e).__name__}: {str(e)[:200]}'}
        records[r['site_id']]=result
        run.mkdir(parents=True,exist_ok=True)
        temp=checkpoint.with_suffix('.tmp');temp.write_text(json.dumps(records,indent=2));temp.replace(checkpoint)
        save_csv(df,records,args.output)
        print(f'[{i}/{len(scheduled)}] {r["site_id"]} {result["validation_status"]} '
              f'corrected={result.get("action_stage_corrected_ft")} {result.get("reason","")}',flush=True)
        if args.sleep>0:time.sleep(args.sleep)
    print('FINISHED',args.output)

if __name__=='__main__':main()
