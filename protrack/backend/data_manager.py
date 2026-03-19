import pandas as pd
import numpy as np
from datetime import datetime, date
import math
from typing import Optional, Dict, Any, List


PROCESS_STEPS = ['수주', '시방', '자재', '생산', '검사', '포장', '출고', 'OTP', '계산서']

# 컬럼 → 공정 단계 매핑 (새 양식 기준)
STEP_DATE_MAP = {
    '수주':  {'planned': None,              'actual': '수주일자'},
    '시방':  {'planned': '시방예상일',      'actual': '시방출도일'},
    '자재':  {'planned': '자재예상일',      'actual': '자재입고일'},
    '생산':  {'planned': '생산예상일',      'actual': '생산완료일'},
    '검사':  {'planned': '품질검사예상일',   'actual': '품질검사일'},
    '포장':  {'planned': None,             'actual': '포장완료일'},
    '출고':  {'planned': '요구납기일',      'actual': '최종납기일'},
    'OTP':  {'planned': 'OTP예상일',       'actual': 'OTP일자'},
    '계산서': {'planned': None,            'actual': '계산서발행일'},
}

# 진척률 가중치 (실적일만, 합계 100)
PROGRESS_WEIGHTS = {
    '수주일자':     5,
    '시방출도일':   10,
    '자재확인일':   10,
    '자재입고일':   10,
    '생산완료일':   20,
    '품질검사일':   10,
    '포장완료일':   10,
    '최종납기일':   10,
    'OTP일자':     10,
    '계산서발행일':  5,
}


def safe_date(val):
    """Convert various date formats to ISO string safely."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, (datetime, date, pd.Timestamp)):
        try:
            return pd.Timestamp(val).strftime('%Y-%m-%d')
        except:
            return None
    if isinstance(val, (int, float)):
        try:
            s = str(int(val))
            if len(s) == 8:
                return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        except:
            pass
    return None


def infer_current_step(row) -> str:
    """실적일이 찍힌 가장 마지막 단계 기준."""
    if pd.notna(row.get('계산서발행일')):
        return '계산서'
    if pd.notna(row.get('OTP일자')):
        return 'OTP'
    if pd.notna(row.get('최종납기일')):
        return '출고'
    if pd.notna(row.get('포장완료일')):
        return '포장'
    if pd.notna(row.get('품질검사일')):
        return '검사'
    if pd.notna(row.get('생산완료일')):
        return '생산'
    if pd.notna(row.get('자재입고일')):
        return '자재'
    if pd.notna(row.get('시방출도일')):
        return '시방'
    if pd.notna(row.get('수주일자')):
        return '수주'
    return '수주'


def infer_status(row) -> str:
    """Determine status: 완료/지연/진행중."""
    today = pd.Timestamp.now()
    
    # 완료: 포장완료 또는 최종납기일(실적출고) 또는 계산서발행
    if pd.notna(row.get('포장완료일')) or pd.notna(row.get('최종납기일')) or pd.notna(row.get('계산서발행일')):
        return '완료'
    
    due_val = row.get('요구납기일')
    if pd.notna(due_val):
        try:
            if today > pd.Timestamp(due_val):
                return '지연'
        except:
            pass
    
    return '진행중'


def calc_progress(row) -> int:
    """처음~마지막 실적일 단계까지 중간 공정 포함 누적 가중치."""
    weight_cols = list(PROGRESS_WEIGHTS.keys())
    last_idx = -1
    for i, col in enumerate(weight_cols):
        if pd.notna(row.get(col)):
            last_idx = i
    if last_idx < 0:
        return 0
    total = sum(PROGRESS_WEIGHTS[col] for col in weight_cols[:last_idx + 1])
    return min(100, total)


def calc_delay_days(row) -> int:
    """Calculate delay days from due date."""
    today = pd.Timestamp.now()
    due_col = '최종납기일' if pd.notna(row.get('최종납기일')) else '요구납기일'
    due_val = row.get(due_col)
    if pd.notna(due_val):
        try:
            due_date = pd.Timestamp(due_val)
            delta = (today - due_date).days
            return max(0, delta)
        except:
            pass
    return 0


class DataManager:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df: pd.DataFrame = pd.DataFrame()
        self._load()

    def _load(self):
        try:
            engine = 'xlrd' if str(self.filepath).endswith('.xls') else 'openpyxl'
            df = pd.read_excel(self.filepath, engine=engine)

            # TLGS 제외
            if '제품군' in df.columns:
                df = df[df['제품군'] != 'TLGS']

            # 컬럼명 정규화
            if 'dlvdt' in df.columns:
                df = df.rename(columns={'dlvdt': '요구납기일'})

            # 날짜 컬럼 자동 감지 + 엑셀 시리얼 숫자 보정
            date_cols = [col for col in df.columns if '일자' in str(col) or str(col).endswith('일')]
            for col in date_cols:
                def fix_date(v):
                    if pd.isna(v):
                        return pd.NaT
                    if isinstance(v, (int, float)):
                        try:
                            s = str(int(v))
                            if len(s) == 8:  # YYYYMMDD 형식
                                return pd.Timestamp(s[:4] + '-' + s[4:6] + '-' + s[6:])
                            # Excel 시리얼 (일반적으로 5자리)
                            return pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(v))
                        except:
                            return pd.NaT
                    return v
                df[col] = df[col].apply(fix_date)
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            self.df = self._enrich(df)
            print(f"[DataManager] Loaded {len(self.df)} rows from {self.filepath}")
        except Exception as e:
            print(f"[DataManager] Load error: {e}")
            self.df = pd.DataFrame()

    def reload(self, filepath: str = None):
        if filepath:
            self.filepath = filepath
        self._load()

    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add computed columns."""
        df = df.copy()
        df['_current_step'] = df.apply(infer_current_step, axis=1)
        df['_status'] = df.apply(infer_status, axis=1)
        df['_progress'] = df.apply(calc_progress, axis=1)
        df['_delay_days'] = df.apply(calc_delay_days, axis=1)
        df['_row_id'] = df.index
        return df

    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert a DataFrame row to a JSON-safe dict."""
        d = {}
        for col, val in row.items():
            if isinstance(val, (pd.Timestamp, datetime, date)):
                d[col] = val.strftime('%Y-%m-%d') if pd.notna(val) else None
            elif isinstance(val, float):
                d[col] = None if math.isnan(val) else val
            elif isinstance(val, np.integer):
                d[col] = int(val)
            elif isinstance(val, np.floating):
                d[col] = None if np.isnan(val) else float(val)
            else:
                d[col] = val
        return d

    def get_filtered_df(
        self,
        search: str = "",
        status_filter: str = "",
        company_filter: str = "",
        step_filter: str = "",
    ) -> pd.DataFrame:
        df = self.df.copy()
        
        if search:
            mask = (
                df['수주번호'].astype(str).str.contains(search, case=False, na=False) |
                df['업체명'].astype(str).str.contains(search, case=False, na=False) |
                df['프로젝트'].astype(str).str.contains(search, case=False, na=False) |
                df['품명'].astype(str).str.contains(search, case=False, na=False) |
                df['시스템명'].astype(str).str.contains(search, case=False, na=False)
            )
            df = df[mask]
        
        if status_filter and status_filter != "전체":
            df = df[df['_status'] == status_filter]
        
        if company_filter and company_filter != "전체":
            df = df[df['업체명'] == company_filter]
        
        if step_filter and step_filter != "전체":
            df = df[df['_current_step'] == step_filter]
        
        return df

    def get_processes(
        self,
        page: int = 1,
        page_size: int = 50,
        search: str = "",
        status_filter: str = "",
        company_filter: str = "",
        step_filter: str = "",
        sort_by: str = "수주번호",
        sort_dir: str = "asc",
    ) -> Dict:
        df = self.get_filtered_df(search, status_filter, company_filter, step_filter)
        
        total = len(df)
        total_pages = max(1, math.ceil(total / page_size))
        page = max(1, min(page, total_pages))
        
        # Sort
        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=(sort_dir == "asc"), na_position='last')
        
        start = (page - 1) * page_size
        end = start + page_size
        page_df = df.iloc[start:end]
        
        items = [self._row_to_dict(row) for _, row in page_df.iterrows()]
        
        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    def get_process_detail(self, order_no: str, ordseq: int) -> Optional[Dict]:
        mask = (self.df['수주번호'] == order_no) & (self.df['ordseq'] == ordseq)
        rows = self.df[mask]
        if rows.empty:
            return None
        row = rows.iloc[0]
        d = self._row_to_dict(row)
        
        # Build timeline
        timeline = []
        for step in PROCESS_STEPS:
            mapping = STEP_DATE_MAP.get(step, {})
            planned_col = mapping.get('planned')
            actual_col = mapping.get('actual')
            planned = safe_date(row.get(planned_col)) if planned_col else None
            actual = safe_date(row.get(actual_col)) if actual_col else None
            
            timeline.append({
                "step": step,
                "planned": planned,
                "actual": actual,
                "is_current": step == row['_current_step'],
                "is_done": actual is not None,
            })
        
        d['_timeline'] = timeline
        return d

    def update_process(self, order_no: str, ordseq: int, updates: Dict) -> bool:
        mask = (self.df['수주번호'] == order_no) & (self.df['ordseq'] == ordseq)
        if not self.df[mask].any().any():
            return False
        
        for col, val in updates.items():
            if col in self.df.columns:
                self.df.loc[mask, col] = val
        
        # Re-enrich the modified rows
        for idx in self.df[mask].index:
            self.df.at[idx, '_current_step'] = infer_current_step(self.df.loc[idx])
            self.df.at[idx, '_status'] = infer_status(self.df.loc[idx])
            self.df.at[idx, '_progress'] = calc_progress(self.df.loc[idx])
            self.df.at[idx, '_delay_days'] = calc_delay_days(self.df.loc[idx])
        
        # Persist to disk
        try:
            save_df = self.df.drop(columns=[c for c in self.df.columns if c.startswith('_')], errors='ignore')
            save_df.to_excel(self.filepath, index=False)
        except Exception as e:
            print(f"[DataManager] Save error: {e}")
        
        return True

    def get_kpi(self) -> Dict:
        df = self.df
        total = len(df)
        in_progress = len(df[df['_status'] == '진행중'])
        delayed = len(df[df['_status'] == '지연'])
        completed = len(df[df['_status'] == '완료'])
        
        return {
            "total": total,
            "in_progress": in_progress,
            "delayed": delayed,
            "completed": completed,
        }

    def get_process_load(self) -> List[Dict]:
        if self.df.empty:
            return []
        today = pd.Timestamp.now()
        result = []
        for step in PROCESS_STEPS:
            step_df = self.df[self.df['_current_step'] == step]
            normal = 0
            warning = 0
            delayed = 0
            planned_col = STEP_DATE_MAP.get(step, {}).get('planned')
            actual_col = STEP_DATE_MAP.get(step, {}).get('actual')
            for _, row in step_df.iterrows():
                actual = row.get(actual_col) if actual_col else None
                planned = row.get(planned_col) if planned_col else None
                if pd.notna(actual):
                    # 실적일 있음 - 예상일 대비 차이 계산
                    if pd.notna(planned):
                        try:
                            diff = (pd.Timestamp(actual) - pd.Timestamp(planned)).days
                            if diff <= 0:
                                normal += 1
                            elif diff <= 3:
                                warning += 1
                            else:
                                delayed += 1
                        except:
                            normal += 1
                    else:
                        normal += 1
                else:
                    # 실적일 없음 - 예상일 초과 여부
                    if pd.notna(planned):
                        try:
                            if today > pd.Timestamp(planned):
                                delayed += 1
                            else:
                                normal += 1
                        except:
                            normal += 1
                    else:
                        normal += 1
            result.append({
                "step": step,
                "count": int(len(step_df)),
                "normal": normal,
                "warning": warning,
                "delayed": delayed,
            })
        return result

    def get_urgent_delays(self, limit: int = 5) -> List[Dict]:
        delayed = self.df[self.df['_status'] == '지연'].copy()
        delayed = delayed.sort_values('_delay_days', ascending=False).head(limit)
        result = []
        for _, row in delayed.iterrows():
            result.append({
                "수주번호": row.get('수주번호', ''),
                "ordseq": int(row.get('ordseq', 0)),
                "업체명": row.get('업체명', ''),
                "품명": row.get('품명', ''),
                "_current_step": row.get('_current_step', ''),
                "_delay_days": int(row.get('_delay_days', 0)),
                "_progress": int(row.get('_progress', 0)),
            })
        return result

    def get_company_distribution(self) -> List[Dict]:
        if self.df.empty:
            return []
        counts = self.df['업체명'].value_counts().head(10)
        return [{"name": k, "value": int(v)} for k, v in counts.items()]

    def get_monthly_trend(self) -> List[Dict]:
        df = self.df.copy()
        col = '수주일자'
        if col not in df.columns:
            return []
        df[col] = pd.to_datetime(df[col], errors='coerce')
        df = df.dropna(subset=[col])
        df['month'] = df[col].dt.to_period('M').astype(str)
        monthly = df.groupby('month').size().reset_index(name='count')
        monthly = monthly.sort_values('month').tail(12)
        return monthly.to_dict(orient='records')

    def get_unique_values(self, col: str) -> List[str]:
        if col not in self.df.columns:
            return []
        vals = self.df[col].dropna().unique().tolist()
        return sorted([str(v) for v in vals])
