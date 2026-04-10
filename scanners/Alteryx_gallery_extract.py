# Paste this into the Alteryx Python Tool
# Close the editor, then click the big green RUN button (Ctrl+R)
# Output #1: Workflow Report
# Output #2: Raw Schedules
# Output #3: Raw Collections (Folders)
# Output #4: Raw Jobs

from ayx import Alteryx
from ayx import Package

Package.installPackages("requests")

import requests
import base64
import json
import pandas as pd
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==============================================================
# CONFIGURATION - FILL IN YOUR API KEY AND SECRET
# ==============================================================

BASE_URL    = "https://prodalteryx.ssg.petsmart.com"
API_KEY     = "your_api_key_here"
API_SECRET  = "your_api_secret_here"
SSL_VERIFY  = False


# ==============================================================
# HELPERS
# ==============================================================

def get_token():
    creds = base64.b64encode(f"{API_KEY}:{API_SECRET}".encode()).decode()
    r = requests.post(
        f"{BASE_URL}/webapi/oauth2/token",
        headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization": f"Basic {creds}"},
        data={"grant_type": "client_credentials"},
        verify=SSL_VERIFY, timeout=30,
    )
    if r.status_code == 200:
        return r.json().get("access_token")
    return None


def api_get(token, endpoint, params=None):
    try:
        r = requests.get(
            f"{BASE_URL}/webapi{endpoint}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            params=params, verify=SSL_VERIFY, timeout=120,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def to_list(r):
    if r is None: return []
    return r if isinstance(r, list) else [r]


def clean_date(dt):
    if not dt:
        return ""
    dt = str(dt)
    if "T" in dt:
        dt = dt.replace("T", " ").split(".")[0]
    return dt


# ==============================================================
# MAIN
# ==============================================================

token = get_token()

if not token:
    Alteryx.write(pd.DataFrame([{"Error": "Auth failed. Check API_KEY and API_SECRET."}]), 1)
else:
    # ----- Pull all data -----
    workflows   = to_list(api_get(token, "/v3/workflows", {"view": "Full"}))
    if not workflows:
        workflows = to_list(api_get(token, "/admin/v1/workflows"))

    schedules   = to_list(api_get(token, "/v3/schedules", {"view": "Full"}))
    if not schedules:
        schedules = to_list(api_get(token, "/admin/v1/schedules"))

    collections = to_list(api_get(token, "/v3/collections", {"view": "Full"}))
    if not collections:
        collections = to_list(api_get(token, "/admin/v1/collections"))

    users       = to_list(api_get(token, "/v3/users", {"view": "Full"}))

    # Pull ALL jobs to get last run dates
    jobs        = to_list(api_get(token, "/v3/jobs"))
    if not jobs:
        jobs = to_list(api_get(token, "/admin/v1/workflows/jobs"))

    # ----- Build workflow ID -> last run date from Jobs -----
    # Find the most recent completed/run job per workflow
    wf_last_run = {}
    for j in jobs:
        if not j or not isinstance(j, dict):
            continue
        wf_id = str(j.get("workflowId", "") or j.get("appId", "") or j.get("workflowid", "") or "")
        if not wf_id:
            continue

        # Get the job completion/create date
        job_date = (j.get("completedDate", "") or
                    j.get("createDate", "") or
                    j.get("endTime", "") or
                    j.get("createDateTime", "") or
                    j.get("completionDateTime", "") or "")

        if not job_date:
            continue

        job_date_str = str(job_date)

        # Keep the most recent date per workflow
        if wf_id not in wf_last_run:
            wf_last_run[wf_id] = job_date_str
        else:
            if job_date_str > wf_last_run[wf_id]:
                wf_last_run[wf_id] = job_date_str

    # ----- Also build last run from schedules (lastRunDate) -----
    schedule_last_run = {}
    for s in schedules:
        if not s or not isinstance(s, dict):
            continue
        wf_id = str(s.get("workflowId", "") or s.get("appId", "") or "")
        sched_last = (s.get("lastRunDate", "") or
                      s.get("lastRun", "") or
                      s.get("lastRunTime", "") or "")
        if wf_id and sched_last:
            sched_last_str = str(sched_last)
            if wf_id not in schedule_last_run:
                schedule_last_run[wf_id] = sched_last_str
            else:
                if sched_last_str > schedule_last_run[wf_id]:
                    schedule_last_run[wf_id] = sched_last_str

    # ----- Build user ID -> name map -----
    user_map = {}
    for u in users:
        if not u or not isinstance(u, dict):
            continue
        uid = str(u.get("id", ""))
        first = u.get("firstName", "") or ""
        last = u.get("lastName", "") or ""
        email = u.get("email", "") or ""
        name = f"{first} {last}".strip() or email
        if uid:
            user_map[uid] = name

    # ----- Build workflow -> collection mappings -----
    wf_to_folder = {}
    wf_to_folder_owner = {}
    wf_to_user_count = {}

    for col in collections:
        if not col or not isinstance(col, dict):
            continue
        folder_name = col.get("name", "") or ""

        # Collection Owner
        col_owner_id = str(col.get("ownerId", "") or col.get("owner", "") or "")
        col_owner_name = user_map.get(col_owner_id, "")
        if not col_owner_name:
            owner_obj = col.get("owner", {})
            if isinstance(owner_obj, dict):
                first = owner_obj.get("firstName", "") or ""
                last = owner_obj.get("lastName", "") or ""
                col_owner_name = f"{first} {last}".strip()
            if not col_owner_name:
                col_owner_name = col.get("ownerName", "") or col_owner_id

        # Workflow IDs in this collection
        col_workflows = col.get("workflows", []) or col.get("apps", []) or []
        if not col_workflows:
            col_workflows = col.get("workflowIds", []) or col.get("appIds", []) or []

        # AD User count
        ad_users = col.get("adUsers", []) or col.get("users", []) or col.get("userIds", []) or []
        if isinstance(ad_users, list):
            user_count = len(ad_users)
        elif isinstance(ad_users, int):
            user_count = ad_users
        else:
            user_count = 0

        for wf_ref in col_workflows:
            if isinstance(wf_ref, dict):
                wf_id = str(wf_ref.get("id", "") or wf_ref.get("workflowId", "") or wf_ref.get("appId", "") or "")
            else:
                wf_id = str(wf_ref)

            if wf_id:
                wf_to_folder[wf_id] = folder_name
                wf_to_folder_owner[wf_id] = col_owner_name
                wf_to_user_count[wf_id] = user_count

    # ----- Scheduled workflow IDs -----
    scheduled_ids = set()
    for s in schedules:
        if not s or not isinstance(s, dict):
            continue
        for key in ["workflowId", "workflowid", "appId", "appid"]:
            val = s.get(key, "")
            if val:
                scheduled_ids.add(str(val))

    # ----- Build report rows -----
    rows = []
    for wf in workflows:
        if not wf or not isinstance(wf, dict):
            continue
        wf_id = str(wf.get("id", "") or wf.get("_id", "") or "")

        # Workflow Name
        wf_name = (wf.get("name", "") or
                   wf.get("metaInfo", {}).get("name", "") or
                   wf.get("fileName", "") or "")

        # Studio
        studio = (wf.get("studioName", "") or
                  wf.get("publishedVersionOwner", {}).get("studioName", "") or
                  wf.get("metaInfo", {}).get("studioName", "") or
                  wf.get("district", "") or "")

        # Folder Name (from Collection)
        folder = wf_to_folder.get(wf_id, "")
        if not folder:
            folder = studio or "Unassigned"

        # Folder Owner
        folder_owner = wf_to_folder_owner.get(wf_id, "")

        # Status
        is_public = wf.get("isPublic", None)
        if is_public is True:
            status = "Published"
        elif is_public is False:
            status = "Private"
        else:
            status = "Published" if wf.get("publishedVersionNumber", "") else wf.get("status", "Active")

        # Workflow Owner
        owner_id = str(wf.get("ownerId", "") or
                       wf.get("owner", "") or
                       wf.get("metaInfo", {}).get("ownerId", "") or "")
        owner = user_map.get(owner_id, "")
        if not owner:
            owner = (wf.get("ownerName", "") or
                     wf.get("userName", "") or
                     wf.get("metaInfo", {}).get("author", "") or owner_id)

        # Published / Updated date
        published_updated = clean_date(
            wf.get("uploadDate", "") or
            wf.get("dateUpdated", "") or
            wf.get("metaInfo", {}).get("uploadDate", "") or
            wf.get("publishDate", "") or
            wf.get("dateCreated", "") or ""
        )

        # Last Refresh - Priority: Jobs > Schedules > Workflow fields
        last_refresh = ""

        # 1) From Jobs (most reliable - actual run history)
        if wf_id in wf_last_run:
            last_refresh = clean_date(wf_last_run[wf_id])

        # 2) From Schedule lastRunDate
        if not last_refresh and wf_id in schedule_last_run:
            last_refresh = clean_date(schedule_last_run[wf_id])

        # 3) Fallback to workflow fields
        if not last_refresh:
            last_refresh = clean_date(
                wf.get("lastRunDate", "") or
                wf.get("metaInfo", {}).get("lastRunDate", "") or ""
            )

        # Schedule Yes/No
        has_schedule = "Yes" if wf_id in scheduled_ids else "No"

        # AD User Count
        user_count = wf_to_user_count.get(wf_id, 0)

        rows.append({
            "Environment":       "PROD",
            "Status":            status,
            "Folder_Name":       folder,
            "Folder_Owner":      folder_owner,
            "Studio":            studio,
            "Workflow_Name":     wf_name,
            "Workflow_Owner":    owner,
            "Published_Updated": published_updated,
            "Last_Refresh":      last_refresh,
            "Schedule":          has_schedule,
            "AD_User_Count":     user_count,
            "Workflow_Id":       wf_id,
        })

    # ----- Output #1: Workflow Report -----
    if rows:
        df = pd.DataFrame(rows)
        df = df.sort_values(["Folder_Name", "Workflow_Name"]).reset_index(drop=True)
        Alteryx.write(df, 1)
    else:
        Alteryx.write(pd.DataFrame([{"Error": "No workflows returned"}]), 1)

    # ----- Output #2: Raw Schedules -----
    flat_sched = []
    for s in schedules:
        flat = {}
        for k, v in s.items():
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    flat[f"{k}_{k2}"] = str(v2) if isinstance(v2, (dict, list)) else v2
            elif isinstance(v, list):
                flat[k] = json.dumps(v)
            else:
                flat[k] = v
        flat_sched.append(flat)
    if flat_sched:
        Alteryx.write(pd.DataFrame(flat_sched), 2)
    else:
        Alteryx.write(pd.DataFrame([{"info": "No schedules found"}]), 2)

    # ----- Output #3: Raw Collections (Folders) -----
    flat_cols = []
    for c in collections:
        flat = {}
        for k, v in c.items():
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    flat[f"{k}_{k2}"] = str(v2) if isinstance(v2, (dict, list)) else v2
            elif isinstance(v, list):
                flat[k] = json.dumps(v)
            else:
                flat[k] = v
        flat_cols.append(flat)
    if flat_cols:
        Alteryx.write(pd.DataFrame(flat_cols), 3)
    else:
        Alteryx.write(pd.DataFrame([{"info": "No collections found"}]), 3)

    # ----- Output #4: Raw Jobs (for reference) -----
    flat_jobs = []
    for j in jobs:
        flat = {}
        for k, v in j.items():
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    flat[f"{k}_{k2}"] = str(v2) if isinstance(v2, (dict, list)) else v2
            elif isinstance(v, list):
                flat[k] = json.dumps(v)
            else:
                flat[k] = v
        flat_jobs.append(flat)
    if flat_jobs:
        Alteryx.write(pd.DataFrame(flat_jobs), 4)
    else:
        Alteryx.write(pd.DataFrame([{"info": "No jobs found"}]), 4)
