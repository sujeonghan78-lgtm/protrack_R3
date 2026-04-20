from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import os
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
dm = DataManager(DATA_FILE)


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
async def get_kpi(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_kpi(product_filter=product_filter)


@app.get("/api/dashboard/process-load")
async def get_process_load(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_process_load(product_filter=product_filter)


@app.get("/api/dashboard/alerts")
async def get_alerts(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_alerts(product_filter=product_filter)


@app.get("/api/dashboard/stage-progress")
async def get_stage_progress(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_stage_progress(product_filter=product_filter)


@app.get("/api/dashboard/stage-by-process")
async def get_stage_by_process(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_stage_by_process(product_filter=product_filter)


@app.get("/api/dashboard/status-distribution")
async def get_status_distribution(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_status_distribution(product_filter=product_filter)


@app.get("/api/dashboard/urgent-delays")
async def get_urgent_delays(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_urgent_delays(limit=5, product_filter=product_filter)


@app.get("/api/dashboard/company-distribution")
async def get_company_distribution(product_filter: str = "", current_user: User = Depends(get_current_user)):
    return dm.get_company_distribution(product_filter=product_filter)


@app.get("/api/dashboard/monthly-delivery")
async def get_monthly_delivery(product_filter: str = "", date_col: str = "요구납기일", current_user: User = Depends(get_current_user)):
    return dm.get_monthly_delivery(product_filter=product_filter, date_col=date_col)


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
    current_user: User = Depends(get_current_user)
):
    return dm.get_processes(page=page, page_size=page_size, search=search, status_filter=status_filter, company_filter=company_filter, step_filter=step_filter, sort_by=sort_by, sort_dir=sort_dir, product_filter=product_filter)


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


# ─── Excel Upload ────────────────────────────────────────────────────────────

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
        
        # ordseq 없으면 수주번호 기준 자동 생성
        if 'ordseq' not in df.columns:
            df['ordseq'] = df.groupby('수주번호').cumcount() + 1

        save_path = os.path.join(os.path.dirname(__file__), "../data/sample.xlsx")
        df.to_excel(save_path, index=False)
        
        dm.reload(save_path)
        return {"message": f"업로드 완료. {len(df)}행 로드됨.", "rows": len(df)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"파일 처리 오류: {str(e)}")


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
