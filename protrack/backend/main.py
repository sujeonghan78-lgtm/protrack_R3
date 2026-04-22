from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import os
import json
import shutil
from datetime import datetime, timedelta
from typing import Optional, List
import math

from auth import (
    authenticate_user, create_access_token, get_current_user,
    require_admin, Token, User, ACCESS_TOKEN_EXPIRE_MINUTES
)
from data_manager import DataManager
from models import ProcessUpdate, PaginationParams

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
MAX_VERSIONS = 10

os.makedirs(VERSIONS_DIR, exist_ok=True)
dm = DataManager(DATA_FILE)


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
async def get_kpi(product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_kpi(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


@app.get("/api/dashboard/process-load")
async def get_process_load(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_process_load(product_filter=product_filter)


@app.get("/api/dashboard/alerts")
async def get_alerts(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_alerts(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


@app.get("/api/dashboard/stage-progress")
async def get_stage_progress(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_stage_progress(product_filter=product_filter)


@app.get("/api/dashboard/stage-by-process")
async def get_stage_by_process(product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_stage_by_process(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


@app.get("/api/dashboard/status-distribution")
async def get_status_distribution(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_status_distribution(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


@app.get("/api/dashboard/urgent-delays")
async def get_urgent_delays(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_urgent_delays(limit=5, product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


@app.get("/api/dashboard/company-distribution")
async def get_company_distribution(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_company_distribution(product_filter=product_filter)


@app.get("/api/dashboard/monthly-delivery")
async def get_monthly_delivery(product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_monthly_delivery(product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


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
    current_user: User = Depends(get_current_user)
):
    return dm.get_processes(page=page, page_size=page_size, search=search, status_filter=status_filter, company_filter=company_filter, step_filter=step_filter, sort_by=sort_by, sort_dir=sort_dir, product_filter=product_filter, date_col=date_col, date_from=date_from, date_to=date_to)


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
    current_user: User = Depends(require_admin)
):
    df = dm.get_filtered_df(search=search, status_filter=status_filter, company_filter=company_filter)
    
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
