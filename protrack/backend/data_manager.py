import pandas as pd
import numpy as np
from datetime import datetime, date
import math
from typing import Optional, Dict, Any, List


PROCESS_STEPS = ['수주', '시방', '자재', '생산', '검사', '포장', '출고', 'OTP', '계산서']

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
    today = pd.Timestamp.now()

    if pd.notna(row.get('계산서발행일')):
        return '완료'

    due_val = row.get('요구납기일')
    if pd.notna(due_val):
        try:
            due_date = pd.Timestamp(due_val)
            delta = (due_date - today).days
            if delta < 0:
                return '지연'
            elif delta <= 7:
                return 'At Risk'
        except:
            pass

    return 'On Track'


def calc_progress(row) -> int:
    weight_cols = list(PROGRESS_WEIGHTS.keys())
    last_idx = -1
    for i, col in enumerate(weight_cols):
        if pd.notna(row.get(col)):
            last_idx = i
    if last_idx < 0:
        return 0
    total = sum(PROGRESS_WEIGHTS[col] for col in weight_cols[:last_idx + 1])
    return min(100, total)


def get_current_next_step_info(row):
    """현재 단계와 다음 단계의 예상/실적일 반환"""
    today = pd.Timestamp.now()
    current_step = row.get('_current_step') or infer_current_step(row)
    
    # 현재 단계 정보
    cur_map = STEP_DATE_MAP.get(current_step, {})
    cur_actual_col = cur_map.get('actual')
    cur_planned_col = cur_map.get('planned')
    cur_actual = row.get(cur_actual_col) if cur_actual_col else None
    cur_planned = row.get(cur_planned_col) if cur_planned_col else None
    
    # 다음 단계 정보
    steps = list(STEP_DATE_MAP.keys())
    cur_idx = steps.index(current_step) if current_step in steps else -1
    next_step = steps[cur_idx + 1] if cur_idx >= 0 and cur_idx + 1 < len(steps) else None
    next_map = STEP_DATE_MAP.get(next_step, {}) if next_step else {}
    next_planned_col = next_map.get('planned')
    next_planned = row.get(next_planned_col) if next_planned_col else None

    return {
        'cur_actual': cur_actual if pd.notna(cur_actual) else None,
        'cur_planned': cur_planned if pd.notna(cur_planned) else None,
        'next_planned': next_planned if pd.notna(next_planned) else None,
        'today': today,
    }


def calc_stage_diff(row) -> dict:
    """현재/다음 단계 날짜 차이 계산"""
    info = get_current_next_step_info(row)
    today = info['today']
    result = {'cur_diff': None, 'cur_has_actual': False, 'next_diff': None}
    
    # 현재 단계: 실적 있으면 실적-예상, 없으면 오늘-예상
    if info['cur_planned']:
        try:
            planned = pd.Timestamp(info['cur_planned'])
            if info['cur_actual']:
                result['cur_diff'] = int((pd.Timestamp(info['cur_actual']) - planned).days)
                result['cur_has_actual'] = True
            else:
                result['cur_diff'] = int((today - planned).days)
                result['cur_has_actual'] = False
        except:
            pass
    
    # 다음 단계: 오늘-예상
    if info['next_planned']:
        try:
            result['next_diff'] = int((today - pd.Timestamp(info['next_planned'])).days)
        except:
            pass
    
    return result


def infer_status(row) -> str:
    """완료: 계산서발행일, 지연/임박/정상: 현재+다음 단계 기준"""
    today = pd.Timestamp.now()

    if pd.notna(row.get('계산서발행일')):
        return '완료'

    diff = calc_stage_diff(row)
    
    # 지연 판단: 현재단계 실적이 예상보다 늦거나, 실적없고 예상일 초과
    cur_diff = diff.get('cur_diff')
    next_diff = diff.get('next_diff')
    
    if cur_diff is not None and cur_diff > 0:
        return '지연'
    if next_diff is not None and next_diff > 0:
        return '지연'
    
    # 임박: 다음 단계 예상일 7일 이내
    if next_diff is not None and next_diff >= -7:
        return 'At Risk'
    if cur_diff is not None and not diff.get('cur_has_actual') and cur_diff >= -7:
        return 'At Risk'

    return 'On Track'


def calc_delay_days(row) -> int:
    diff = calc_stage_diff(row)
    cur_diff = diff.get('cur_diff') or 0
    next_diff = diff.get('next_diff') or 0
    return max(0, cur_diff, next_diff)


class DataManager:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df: pd.DataFrame = pd.DataFrame()
        self._load()

    def _load(self):
        try:
            engine = 'xlrd' if str(self.filepath).endswith('.xls') else 'openpyxl'
            df = pd.read_excel(self.filepath, engine=engine)

            if '제품군' in df.columns:
                df = df[df['제품군'] != 'TLGS']

            if 'dlvdt' in df.columns:
                df = df.rename(columns={'dlvdt': '요구납기일'})

            if 'ordseq' not in df.columns:
                df['ordseq'] = df.groupby('수주번호').cumcount() + 1

            date_cols = [col for col in df.columns if '일자' in str(col) or str(col).endswith('일')]
            for col in date_cols:
                def fix_date(v):
                    if pd.isna(v):
                        return pd.NaT
                    if isinstance(v, (int, float)):
                        try:
                            s = str(int(v))
                            if len(s) == 8:
                                return pd.Timestamp(s[:4] + '-' + s[4:6] + '-' + s[6:])
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
        df = df.copy()
        df['_current_step'] = df.apply(infer_current_step, axis=1)

        def enrich_row(row):
            diff = calc_stage_diff(row)
            return pd.Series({
                '_status': infer_status(row),
                '_progress': calc_progress(row),
                '_delay_days': calc_delay_days(row),
                '_cur_diff': diff.get('cur_diff'),
                '_cur_has_actual': diff.get('cur_has_actual', False),
                '_next_diff': diff.get('next_diff'),
            })

        enriched = df.apply(enrich_row, axis=1)
        df = pd.concat([df, enriched], axis=1)
        df['_row_id'] = df.index
        return df

    def _row_to_dict(self, row) -> Dict[str, Any]:
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

    def get_filtered_df(self, search="", status_filter="", company_filter="", step_filter="", product_filter="") -> pd.DataFrame:
        df = self.df.copy()

        if search:
            mask = (
                df['수주번호'].astype(str).str.contains(search, case=False, na=False) |
                df['업체명'].astype(str).str.contains(search, case=False, na=False) |
                df['프로젝트'].astype(str).str.contains(search, case=False, na=False) |
                df['시스템명'].astype(str).str.contains(search, case=False, na=False) if '시스템명' in df.columns else pd.Series(False, index=df.index) |
                df['시스템명'].astype(str).str.contains(search, case=False, na=False)
            )
            df = df[mask]

        if status_filter and status_filter != "전체":
            df = df[df['_status'] == status_filter]

        if company_filter and company_filter != "전체":
            df = df[df['업체명'] == company_filter]

        if step_filter and step_filter != "전체":
            df = df[df['_current_step'] == step_filter]

        if product_filter and product_filter != "전체" and '제품군' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['제품군'].isin(pf_list)]

        return df

    def get_processes(self, page=1, page_size=50, search="", status_filter="", company_filter="", step_filter="", sort_by="수주번호", sort_dir="asc", product_filter="") -> Dict:
        df = self.get_filtered_df(search, status_filter, company_filter, step_filter, product_filter)

        total = len(df)
        total_pages = max(1, math.ceil(total / page_size))
        page = max(1, min(page, total_pages))

        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=(sort_dir == "asc"), na_position='last')

        start = (page - 1) * page_size
        items = [self._row_to_dict(row) for _, row in df.iloc[start:start+page_size].iterrows()]

        return {"items": items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}

    def get_process_detail(self, order_no: str, ordseq: int) -> Optional[Dict]:
        mask = (self.df['수주번호'] == order_no) & (self.df['ordseq'] == ordseq)
        rows = self.df[mask]
        if rows.empty:
            return None
        row = rows.iloc[0]
        d = self._row_to_dict(row)

        timeline = []
        for step in PROCESS_STEPS:
            mapping = STEP_DATE_MAP.get(step, {})
            planned_col = mapping.get('planned')
            actual_col = mapping.get('actual')
            planned = safe_date(row.get(planned_col)) if planned_col else None
            actual = safe_date(row.get(actual_col)) if actual_col else None
            timeline.append({"step": step, "planned": planned, "actual": actual, "is_current": step == row['_current_step'], "is_done": actual is not None})

        d['_timeline'] = timeline
        return d

    def update_process(self, order_no: str, ordseq: int, updates: Dict) -> bool:
        mask = (self.df['수주번호'] == order_no) & (self.df['ordseq'] == ordseq)
        if not self.df[mask].any().any():
            return False

        for col, val in updates.items():
            if col in self.df.columns:
                self.df.loc[mask, col] = val

        for idx in self.df[mask].index:
            self.df.at[idx, '_current_step'] = infer_current_step(self.df.loc[idx])
            self.df.at[idx, '_status'] = infer_status(self.df.loc[idx])
            self.df.at[idx, '_progress'] = calc_progress(self.df.loc[idx])
            self.df.at[idx, '_delay_days'] = calc_delay_days(self.df.loc[idx])

        try:
            save_df = self.df.drop(columns=[c for c in self.df.columns if c.startswith('_')], errors='ignore')
            save_df.to_excel(self.filepath, index=False)
        except Exception as e:
            print(f"[DataManager] Save error: {e}")

        return True

    def get_kpi(self, product_filter: str = "") -> Dict:
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '제품군' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['제품군'].isin(pf_list)]

        total = len(df)
        on_track = len(df[df['_status'] == 'On Track'])
        at_risk = len(df[df['_status'] == 'At Risk'])
        delayed = len(df[df['_status'] == '지연'])
        completed = len(df[df['_status'] == '완료'])
        avg_progress = int(df['_progress'].mean()) if total > 0 else 0

        return {"total": total, "on_track": on_track, "at_risk": at_risk, "delayed": delayed, "completed": completed, "avg_progress": avg_progress}

    def get_process_load(self, product_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '제품군' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['제품군'].isin(pf_list)]

        today = pd.Timestamp.now()
        result = []
        for step in PROCESS_STEPS:
            step_df = df[df['_current_step'] == step]
            normal = 0; warning = 0; delayed = 0
            planned_col = STEP_DATE_MAP.get(step, {}).get('planned')
            actual_col = STEP_DATE_MAP.get(step, {}).get('actual')
            for _, row in step_df.iterrows():
                actual = row.get(actual_col) if actual_col else None
                planned = row.get(planned_col) if planned_col else None
                if pd.notna(actual):
                    if pd.notna(planned):
                        try:
                            diff = (pd.Timestamp(actual) - pd.Timestamp(planned)).days
                            if diff <= 0: normal += 1
                            elif diff <= 3: warning += 1
                            else: delayed += 1
                        except: normal += 1
                    else: normal += 1
                else:
                    if pd.notna(planned):
                        try:
                            if today > pd.Timestamp(planned): delayed += 1
                            else: normal += 1
                        except: normal += 1
                    else: normal += 1
            result.append({"step": step, "count": int(len(step_df)), "normal": normal, "warning": warning, "delayed": delayed})
        return result

    def get_stage_progress(self, product_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '제품군' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['제품군'].isin(pf_list)]
        if '시스템명' not in df.columns:
            return []

        result = []
        for system, group in df.groupby('시스템명'):
            total = len(group)
            completed = len(group[group['_status'] == '완료'])
            rate = int(completed / total * 100) if total > 0 else 0
            result.append({"system": str(system), "total": total, "completed": completed, "rate": rate})
        result.sort(key=lambda x: x['rate'], reverse=True)
        return result

    def get_alerts(self, product_filter: str = "") -> Dict:
        if self.df.empty:
            return {"delayed": [], "at_risk": [], "due_soon": {"출고": [], "OTP": []}}

        df = self.df.copy()
        if product_filter and product_filter != "전체" and '제품군' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['제품군'].isin(pf_list)]

        today = pd.Timestamp.now()
        this_month_start = today.replace(day=1)
        next_month_start = this_month_start + pd.DateOffset(months=1)

        def row_summary(row):
            return {
                "수주번호": row.get('수주번호', ''),
                "ordseq": int(row.get('ordseq', 0)),
                "업체명": row.get('업체명', ''),
                "품명": row.get('품명', ''),
                "_current_step": row.get('_current_step', ''),
                "_progress": int(row.get('_progress', 0)),
                "요구납기일": safe_date(row.get('요구납기일')),
                "OTP예상일": safe_date(row.get('OTP예상일')),
            }

        delayed = [row_summary(row) for _, row in df[df['_status'] == '지연'].sort_values('_delay_days', ascending=False).iterrows()]
        at_risk = [row_summary(row) for _, row in df[df['_status'] == 'At Risk'].iterrows()]

        due_soon_출고 = []
        if '요구납기일' in df.columns:
            mask = df['요구납기일'].notna() & (df['요구납기일'] >= this_month_start) & (df['요구납기일'] < next_month_start) & (df['_status'] != '완료')
            due_soon_출고 = [row_summary(row) for _, row in df[mask].iterrows()]

        due_soon_otp = []
        if 'OTP예상일' in df.columns:
            mask = df['OTP예상일'].notna() & (df['OTP예상일'] >= this_month_start) & (df['OTP예상일'] < next_month_start) & (df['_status'] != '완료')
            due_soon_otp = [row_summary(row) for _, row in df[mask].iterrows()]

        return {"delayed": delayed, "at_risk": at_risk, "due_soon": {"출고": due_soon_출고, "OTP": due_soon_otp}}

    def get_company_distribution(self, product_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '제품군' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['제품군'].isin(pf_list)]
        counts = df['업체명'].value_counts().head(10)
        return [{"name": k, "value": int(v)} for k, v in counts.items()]

    def get_urgent_delays(self, limit: int = 5) -> List[Dict]:
        delayed = self.df[self.df['_status'] == '지연'].copy()
        delayed = delayed.sort_values('_delay_days', ascending=False).head(limit)
        result = []
        for _, row in delayed.iterrows():
            result.append({"수주번호": row.get('수주번호', ''), "ordseq": int(row.get('ordseq', 0)), "업체명": row.get('업체명', ''), "품명": row.get('품명', ''), "_current_step": row.get('_current_step', ''), "_delay_days": int(row.get('_delay_days', 0)), "_progress": int(row.get('_progress', 0))})
        return result

    def get_unique_values(self, col: str) -> List[str]:
        if col not in self.df.columns:
            return []
        vals = self.df[col].dropna().unique().tolist()
        return sorted([str(v) for v in vals])
