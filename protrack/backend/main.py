from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import os
import json
import shutil
import unicodedata
import uuid
from datetime import datetime, timedelta
from typing import Optional, List
import math

from auth import (
    authenticate_user, create_access_token, get_current_user,
    require_admin, Token, User, ACCESS_TOKEN_EXPIRE_MINUTES
)
from data_manager import DataManager
from models import ProcessUpdate, PaginationParams, DelayReasonUpdate, DelayReasonCreate

app = FastAPI(title="PRO-TRACK API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_FILE = os.path.join(os.path.dirname(__file__), "../data/sample.xlsx")
VERSIONS_DIR = os.path.join(os.path.dirname(__file__), "../data/versions")
VERSIONS_META = os.path.join(os.path.dirname(__file__), "../data/versions.json")
DELAY_REASONS_FILE = os.path.join(os.path.dirname(__file__), "../data/delay_reasons.json")
MAX_VERSIONS = 10

os.makedirs(VERSIONS_DIR, exist_ok=True)
import time as _time
_t0 = _time.time()
dm = DataManager(DATA_FILE)
print(f"[startup] DataManager 초기 로딩 완료: {round(_time.time() - _t0, 1)}초, {len(dm.df)}행 (파일: {DATA_FILE})")


def load_versions() -> list:
    if not os.path.exists(VERSIONS_META):
        return []
    try:
        with open(VERSIONS_META, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return []


def save_versions(versions: list):
    with open(VERSIONS_META, 'w', encoding='utf-8') as f:
        json.dump(versions, f, ensure_ascii=False, indent=2)


# ─── 지연 사유 영구 저장소 ────────────────────────────────────────────────────
# 엑셀 재업로드/dm.reload와 무관하게 유지되는 별도 로그. 항목마다 고유 id를 가지며,
# 수주번호+시스템명을 기준으로 여러 건(여러 단계, 같은 단계라도 여러 번)이 쌓일 수 있음.
# {entry_id: {id, 수주번호, 시스템명, _current_step, reason, updated_at, updated_by}}

def load_delay_reasons() -> dict:
    if not os.path.exists(DELAY_REASONS_FILE):
        return {}
    try:
        with open(DELAY_REASONS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except:
        return {}
    # 이전 스키마(수주번호::시스템명::현재단계 형태의 키만 있고 id 필드가 없던 버전) 호환 —
    # 기존 딕셔너리 키를 그대로 id로 사용
    changed = False
    for k, v in data.items():
        if isinstance(v, dict) and not v.get('id'):
            v['id'] = k
            changed = True
    if changed:
        save_delay_reasons(data)
    return data


def save_delay_reasons(data: dict):
    with open(DELAY_REASONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _group_reasons(reasons: dict) -> dict:
    """(수주번호, 시스템명) 기준으로 사유 항목들을 묶어서 반환"""
    groups = {}
    for v in reasons.values():
        gkey = (v.get('수주번호', ''), v.get('시스템명', ''))
        groups.setdefault(gkey, []).append(v)
    return groups


def attach_reasons(items: list, reasons: dict = None) -> list:
    """공정 목록/지연 모달 등에서 재사용 — 수주번호+시스템명 기준으로 가장 최근 작성된
    지연사유를 대표로 매칭해 _reason 필드를 붙임(작성된 단계가 현재단계와 달라도 최신 사유를 표시)"""
    if reasons is None:
        reasons = load_delay_reasons()
    groups = _group_reasons(reasons)
    for it in items:
        gkey = (it.get('수주번호', ''), it.get('시스템명', ''))
        entries = groups.get(gkey, [])
        rep = max(entries, key=lambda e: e.get('updated_at') or '') if entries else None
        it['_reason'] = rep.get('reason', '') if rep else ''
    return items


# ─── Auth ───────────────────────────────────────────────────────────────────

@app.post("/api/auth/login", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="아이디 또는 비밀번호가 올바르지 않습니다.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username, "role": user.role},
        expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer", "role": user.role, "username": user.username}


@app.get("/api/auth/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {"username": current_user.username, "role": current_user.role}


# ─── KPI & Dashboard ─────────────────────────────────────────────────────────

@app.get("/api/dashboard/kpi")
async def get_kpi(product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_kpi(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)


@app.get("/api/dashboard/process-load")
async def get_process_load(product_filter: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_process_load(product_filter=product_filter, vendor_filter=vendor_filter)


@app.get("/api/dashboard/alerts")
async def get_alerts(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_alerts(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)


@app.get("/api/dashboard/stage-progress")
async def get_stage_progress(product_filter: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_stage_progress(product_filter=product_filter, vendor_filter=vendor_filter)


@app.get("/api/dashboard/stage-by-process")
async def get_stage_by_process(product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_stage_by_process(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)


@app.get("/api/dashboard/stage-delayed-items")
async def get_stage_delayed_items(step: str, product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    items = dm.get_stage_delayed_items(step=step, product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)
    return attach_reasons(items)


# ─── 지연 관리 (수주번호+시스템명 기준, 사유는 여러 건 누적 가능한 로그) ──────────

@app.get("/api/delay-management")
async def get_delay_management(product_filter: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    items = dm.get_all_delayed_items(product_filter=product_filter, vendor_filter=vendor_filter)
    reasons = load_delay_reasons()
    groups = _group_reasons(reasons)
    for it in items:
        order_no = it.get('수주번호', '')
        system_name = it.get('시스템명', '')
        entries = sorted(groups.get((order_no, system_name), []), key=lambda e: e.get('updated_at') or '', reverse=True)
        rep = entries[0] if entries else None
        it['_reason'] = rep.get('reason', '') if rep else ''
        it['_reason_updated_at'] = rep.get('updated_at') if rep else None
        it['_reason_updated_by'] = rep.get('updated_by') if rep else None
        it['_reason_step'] = rep.get('_current_step') if rep else None
        it['_entries'] = entries  # 전체 사유 이력(현재단계 포함) — 각 항목에 id, _current_step, reason, updated_at, updated_by
    return items


@app.post("/api/delay-management/reason")
async def create_delay_reason(body: DelayReasonCreate, current_user: User = Depends(require_admin)):
    reasons = load_delay_reasons()
    entry_id = uuid.uuid4().hex
    reasons[entry_id] = {
        "id": entry_id,
        "수주번호": body.order_no,
        "시스템명": body.system_name,
        "_current_step": body.step,
        "reason": body.reason,
        "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "updated_by": current_user.username,
    }
    save_delay_reasons(reasons)
    return {"success": True, "entry": reasons[entry_id]}


@app.put("/api/delay-management/reason/{entry_id}")
async def update_delay_reason_entry(entry_id: str, body: DelayReasonUpdate, current_user: User = Depends(require_admin)):
    reasons = load_delay_reasons()
    if entry_id not in reasons:
        raise HTTPException(status_code=404, detail="사유를 찾을 수 없습니다.")
    reasons[entry_id]["reason"] = body.reason
    reasons[entry_id]["updated_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    reasons[entry_id]["updated_by"] = current_user.username
    save_delay_reasons(reasons)
    return {"success": True, "entry": reasons[entry_id]}


@app.delete("/api/delay-management/reason/{entry_id}")
async def delete_delay_reason_entry(entry_id: str, current_user: User = Depends(require_admin)):
    reasons = load_delay_reasons()
    if entry_id in reasons:
        del reasons[entry_id]
        save_delay_reasons(reasons)
    return {"success": True}



@app.get("/api/dashboard/status-distribution")
async def get_status_distribution(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_status_distribution(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)


@app.get("/api/dashboard/urgent-delays")
async def get_urgent_delays(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_urgent_delays(limit=5, product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)


@app.get("/api/dashboard/company-distribution")
async def get_company_distribution(product_filter: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_company_distribution(product_filter=product_filter)


@app.get("/api/dashboard/monthly-delivery")
async def get_monthly_delivery(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_monthly_delivery(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)


@app.get("/api/dashboard/monthly-trend")
async def get_monthly_trend(current_user: User = Depends(get_current_user)):
    return dm.get_monthly_trend() if hasattr(dm, 'get_monthly_trend') else []


# ─── Process List ─────────────────────────────────────────────────────────────

@app.get("/api/processes")
async def get_processes(
    page: int = 1,
    page_size: int = 50,
    search: str = "",
    status_filter: str = "",
    company_filter: str = "",
    step_filter: str = "",
    sort_by: str = "수주번호",
    sort_dir: str = "asc",
    product_filter: str = "",
    date_col: str = "",
    date_from: str = "",
    date_to: str = "",
    vendor_filter: str = "",
    current_user: User = Depends(get_current_user)
):
    result = dm.get_processes(page=page, page_size=page_size, search=search, status_filter=status_filter, company_filter=company_filter, step_filter=step_filter, sort_by=sort_by, sort_dir=sort_dir, product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to, vendor_filter=vendor_filter)
    result["items"] = attach_reasons(result.get("items", []))
    return result


@app.get("/api/processes/grouped")
async def get_grouped_processes(
    page: int = 1,
    page_size: int = 20,
    search: str = "",
    status_filter: str = "",
    company_filter: str = "",
    step_filter: str = "",
    product_filter: str = "",
    vendor_filter: str = "",
    sort_by: str = "수주번호",
    sort_dir: str = "asc",
    current_user: User = Depends(get_current_user)
):
    """공정 목록 탭 — 수주번호 → 차수 → 아이템 계층 구조."""
    result = dm.get_grouped_processes(page=page, page_size=page_size, search=search, status_filter=status_filter, company_filter=company_filter, step_filter=step_filter, product_filter=product_filter, vendor_filter=vendor_filter, sort_by=sort_by, sort_dir=sort_dir)
    return result


@app.post("/api/refresh")
async def refresh_cache(current_user: User = Depends(get_current_user)):
    """[BOM개편] 상태/지연 재계산 캐시(최대 10분) 수동 무효화 — 다음 조회 때 바로 반영되게 함."""
    dm._invalidate_cache()
    return {"message": "새로고침 완료. 다음 조회부터 최신 상태로 반영됩니다."}


@app.get("/api/processes/{order_no}/{ordseq}")
async def get_process_detail(
    order_no: str,
    ordseq: int,
    current_user: User = Depends(get_current_user)
):
    detail = dm.get_process_detail(order_no, ordseq)
    if not detail:
        raise HTTPException(status_code=404, detail="항목을 찾을 수 없습니다.")
    return detail


@app.put("/api/processes/{order_no}/{ordseq}")
async def update_process(
    order_no: str,
    ordseq: int,
    update_data: ProcessUpdate,
    current_user: User = Depends(require_admin)
):
    success = dm.update_process(order_no, ordseq, update_data.dict(exclude_none=True))
    if not success:
        raise HTTPException(status_code=404, detail="항목을 찾을 수 없습니다.")
    return {"message": "업데이트되었습니다.", "success": True}


# ─── Filter Options ──────────────────────────────────────────────────────────

@app.get("/api/filters/companies")
async def get_companies(current_user: User = Depends(get_current_user)):
    return dm.get_unique_values("업체명")


@app.get("/api/filters/projects")
async def get_projects(current_user: User = Depends(get_current_user)):
    return dm.get_unique_values("프로젝트")


@app.get("/api/filters/products")
async def get_products(current_user: User = Depends(get_current_user)):
    return dm.get_unique_values("시스템명")


# ─── Excel Upload (버전 관리 포함) ───────────────────────────────────────────

@app.post("/api/upload")
async def upload_excel(
    file: UploadFile = File(...),
    current_user: User = Depends(require_admin)
):
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="엑셀 파일(.xlsx, .xls)만 업로드 가능합니다.")

    contents = await file.read()
    try:
        engine = 'xlrd' if file.filename.lower().endswith('.xls') else 'openpyxl'
        df = pd.read_excel(io.BytesIO(contents), engine=engine)
        df.columns = [unicodedata.normalize('NFC', str(c)).strip() for c in df.columns]
        required_cols = ['수주번호', '업체명']
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise HTTPException(status_code=400, detail=f"필수 컬럼 누락: {', '.join(missing)}")

        if 'ordseq' not in df.columns:
            df['ordseq'] = df.groupby('수주번호').cumcount() + 1

        # 버전 파일로 저장
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        version_filename = f"v_{ts}.xlsx"
        version_path = os.path.join(VERSIONS_DIR, version_filename)
        df.to_excel(version_path, index=False)
        file_size = os.path.getsize(version_path)

        # 메타데이터 기록
        versions = load_versions()
        new_version = {
            "id": ts,
            "filename": file.filename,
            "stored_as": version_filename,
            "uploaded_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "rows": len(df),
            "size_bytes": file_size,
            "is_active": True,
            "uploaded_by": current_user.username,
        }
        # 기존 버전 비활성화
        for v in versions:
            v["is_active"] = False
        versions.insert(0, new_version)

        # 최대 10개 초과 시 오래된 것 삭제
        if len(versions) > MAX_VERSIONS:
            for old in versions[MAX_VERSIONS:]:
                old_path = os.path.join(VERSIONS_DIR, old["stored_as"])
                if os.path.exists(old_path):
                    os.remove(old_path)
            versions = versions[:MAX_VERSIONS]

        save_versions(versions)

        # 현재 활성 데이터로 적용
        shutil.copy2(version_path, DATA_FILE)
        dm.reload(DATA_FILE)

        return {"message": f"업로드 완료. {len(df)}행 로드됨.", "rows": len(df), "version_id": ts}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파일 처리 오류: {str(e)}")


@app.get("/api/dashboard/summary")
async def get_summary(current_user: User = Depends(get_current_user)):
    summary = dm.get_summary()
    # 마지막 업로드 일시
    versions = load_versions()
    last_upload = versions[0].get("uploaded_at") if versions else None
    last_file = versions[0].get("filename") if versions else None
    summary["last_upload"] = last_upload
    summary["last_file"] = last_file
    return summary


@app.get("/api/versions")
async def get_versions(current_user: User = Depends(get_current_user)):
    versions = load_versions()
    return versions


@app.post("/api/versions/{version_id}/activate")
async def activate_version(
    version_id: str,
    current_user: User = Depends(require_admin)
):
    versions = load_versions()
    target = next((v for v in versions if v["id"] == version_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="버전을 찾을 수 없습니다.")

    version_path = os.path.join(VERSIONS_DIR, target["stored_as"])
    if not os.path.exists(version_path):
        raise HTTPException(status_code=404, detail="버전 파일이 존재하지 않습니다.")

    for v in versions:
        v["is_active"] = (v["id"] == version_id)
    save_versions(versions)

    shutil.copy2(version_path, DATA_FILE)
    dm.reload(DATA_FILE)

    return {"message": f"버전 {version_id} 활성화 완료."}


@app.delete("/api/versions/{version_id}")
async def delete_version(
    version_id: str,
    current_user: User = Depends(require_admin)
):
    versions = load_versions()
    target = next((v for v in versions if v["id"] == version_id), None)
    if not target:
        raise HTTPException(status_code=404, detail="버전을 찾을 수 없습니다.")
    if target.get("is_active"):
        raise HTTPException(status_code=400, detail="활성 버전은 삭제할 수 없습니다.")

    version_path = os.path.join(VERSIONS_DIR, target["stored_as"])
    if os.path.exists(version_path):
        os.remove(version_path)

    versions = [v for v in versions if v["id"] != version_id]
    save_versions(versions)
    return {"message": "삭제 완료."}



# ─── Excel Download ──────────────────────────────────────────────────────────

@app.get("/api/export")
async def export_excel(
    search: str = "",
    status_filter: str = "",
    company_filter: str = "",
    step_filter: str = "",
    product_filter: str = "",
    vendor_filter: str = "",
    date_col: str = "",
    date_from: str = "",
    date_to: str = "",
    current_user: User = Depends(require_admin)
):
    df = dm.get_filtered_df(search=search, status_filter=status_filter, company_filter=company_filter, step_filter=step_filter, product_filter=product_filter, vendor_filter=vendor_filter)
    if date_col and (date_from or date_to):
        from data_manager import apply_date_range
        df = apply_date_range(df, date_col, date_from, date_to)
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='공정현황')
    output.seek(0)
    
    filename = f"protrack_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )




# ─── Vendor Management ───────────────────────────────────────────────────────

VENDORS_FILE = os.path.join(os.path.dirname(__file__), "../data/vendors.json")

def load_vendors() -> dict:
    if not os.path.exists(VENDORS_FILE):
        return {}
    try:
        with open(VENDORS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def save_vendors(vendors: dict):
    with open(VENDORS_FILE, 'w', encoding='utf-8') as f:
        json.dump(vendors, f, ensure_ascii=False, indent=2)

@app.get("/api/vendors")
async def get_vendors(current_user: User = Depends(get_current_user)):
    vendors = load_vendors()
    return [{"name": k, "type": v} for k, v in sorted(vendors.items())]

@app.put("/api/vendors/{name}")
async def update_vendor(name: str, data: dict, current_user: User = Depends(require_admin)):
    vendors = load_vendors()
    vendors[name] = data.get("type", "국내")
    save_vendors(vendors)
    dm.reload_vendors()
    return {"ok": True}

@app.post("/api/vendors")
async def add_vendor(data: dict, current_user: User = Depends(require_admin)):
    name = data.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="업체명을 입력하세요.")
    vendors = load_vendors()
    vendors[name] = data.get("type", "국내")
    save_vendors(vendors)
    dm.reload_vendors()
    return {"ok": True}

@app.delete("/api/vendors/{name}")
async def delete_vendor(name: str, current_user: User = Depends(require_admin)):
    vendors = load_vendors()
    if name in vendors:
        del vendors[name]
        save_vendors(vendors)
        dm.reload_vendors()
    return {"ok": True}

@app.post("/api/vendors/upload")
async def upload_vendors(
    file: UploadFile = File(...),
    current_user: User = Depends(require_admin)
):
    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="엑셀 파일만 업로드 가능합니다.")
    contents = await file.read()
    try:
        engine = 'xlrd' if file.filename.lower().endswith('.xls') else 'openpyxl'
        df = pd.read_excel(io.BytesIO(contents), engine=engine, header=0)
        # A열: 업체명, B열: 구분
        if df.shape[1] < 2:
            raise HTTPException(status_code=400, detail="A열(업체명), B열(구분) 형식이어야 합니다.")
        vendors = load_vendors()
        count = 0
        for _, row in df.iterrows():
            name = str(row.iloc[0]).strip()
            vtype = str(row.iloc[1]).strip()
            if name and name != 'nan' and vtype in ('국내', '해외'):
                vendors[name] = vtype
                count += 1
        save_vendors(vendors)
        dm.reload_vendors()
        return {"message": f"{count}개 업체 등록 완료.", "count": count}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
