import pandas as pd
import numpy as np
from datetime import datetime, date
import math
from typing import Optional, Dict, Any, List


PROCESS_STEPS = ['수주', '시방', '자재', '생산', '검사', '포장', '출고', 'OTP', '계산서']

STEP_DATE_MAP = {
    '수주':  {'planned': None,              'actual': '수주일자'},
    '시방':  {'planned': '시방예상일',      'actual': '시방출도일'},
    '자재':  {'planned': None,             'actual': '자재입고일'},   # 데이터 미운용
    '생산':  {'planned': '생산예상일',      'actual': '생산완료일'},
    '검사':  {'planned': None,             'actual': '품질검사일'},   # 데이터 미운용
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
    """진행 중인 단계(실적이 없는 첫 번째 단계)를 반환.
    자재/검사는 데이터 미운용 단계 — 실적 없어도 스킵.
    모든 단계가 완료된 경우 마지막 단계('계산서')를 반환."""
    # 데이터 미운용 단계: 실적 컬럼이 있어도 비어있으면 다음 단계로 진행
    SKIP_STEPS = {'자재', '검사'}

    actual_cols = [
        ('수주',  '수주일자'),
        ('시방',  '시방출도일'),
        ('자재',  '자재입고일'),
        ('생산',  '생산완료일'),
        ('검사',  '품질검사일'),
        ('포장',  '포장완료일'),
        ('출고',  '최종납기일'),
        ('OTP',  'OTP일자'),
        ('계산서', '계산서발행일'),
    ]
    for step, col in actual_cols:
        if step in SKIP_STEPS:
            continue  # 자재/검사는 실적 없어도 현재 단계로 잡지 않음
        if pd.isna(row.get(col)):
            return step
    return '계산서'


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
    """현재/다음 단계 날짜 차이 계산.
    planned 없는 단계(수주/포장/계산서)는 요구납기일로 fallback."""
    info = get_current_next_step_info(row)
    today = info['today']
    result = {'cur_diff': None, 'cur_has_actual': False, 'next_diff': None}

    # 현재 단계: 실적 있으면 실적-예상, 없으면 오늘-예상
    cur_planned = info['cur_planned']
    # planned 없는 단계 → 요구납기일 fallback
    if not cur_planned:
        due = row.get('요구납기일')
        if due is not None and pd.notna(due):
            cur_planned = due

    if cur_planned:
        try:
            planned = pd.Timestamp(cur_planned)
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
    """완료: 계산서발행일 또는 최종납기일(출고완료)
    지연/임박/정상: 현재+다음 단계 기준"""
    today = pd.Timestamp.now()

    if pd.notna(row.get('계산서발행일')):
        return '완료'
    # 최종납기일(출고)이 있으면 실질 납품 완료
    if pd.notna(row.get('최종납기일')):
        return '완료'

    diff = calc_stage_diff(row)
    cur_diff  = diff.get('cur_diff')
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
    """현재 단계 기준 지연일수.
    실적 있음: 실적일 - 예상일 (양수면 지연)
    실적 없음: 오늘 - 예상일 (양수면 지연)
    예상일 없음: 요구납기일 기준 fallback"""
    today = pd.Timestamp.now()
    current_step = row.get('_current_step') or infer_current_step(row)

    # 완료 건은 0
    if row.get('_status') == '완료' or pd.notna(row.get('계산서발행일')):
        return 0

    step_map = STEP_DATE_MAP.get(current_step, {})
    planned_col = step_map.get('planned')
    actual_col  = step_map.get('actual')

    if planned_col:
        planned = row.get(planned_col)
        if planned is not None and pd.notna(planned):
            actual = row.get(actual_col) if actual_col else None
            try:
                if actual is not None and pd.notna(actual):
                    diff = (pd.Timestamp(actual) - pd.Timestamp(planned)).days
                else:
                    diff = (today - pd.Timestamp(planned)).days
                return max(0, diff)
            except:
                pass

    # fallback: 요구납기일 기준
    due = row.get('요구납기일')
    if due is not None and pd.notna(due):
        try:
            return max(0, (today - pd.Timestamp(due)).days)
        except:
            pass
    return 0


def apply_date_range(df: pd.DataFrame, date_col: str, date_from: str, date_to: str) -> pd.DataFrame:
    """date_col 기준으로 date_from~date_to 범위 필터 적용.
    date_col 값이 없는(NaT/None) 행은 제외 - 해당 날짜 실적이 없는 건은 조회하지 않음."""
    if date_col not in df.columns:
        return df
    if not date_from and not date_to:
        return df
    # 날짜가 없는 행은 제외 (isna 조건 제거)
    df = df[df[date_col].notna()]
    if date_from:
        try:
            df = df[df[date_col] >= pd.Timestamp(date_from)]
        except: pass
    if date_to:
        try:
            df = df[df[date_col] <= pd.Timestamp(date_to)]
        except: pass
    return df

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
            if '시스템명' in df.columns:
                df = df[df['시스템명'] != 'TLGS']

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
            # _current_step이 이미 세팅된 row를 기반으로 계산
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
            elif isinstance(val, (np.bool_, bool)):
                d[col] = bool(val)
            else:
                d[col] = val
        return d

    def _refresh_dynamic(self, df: pd.DataFrame) -> pd.DataFrame:
        """조회 시점의 오늘 날짜로 상태·지연일수를 재계산."""
        df = df.copy()
        # _current_step은 실적 기반이라 날짜 무관 — 재계산 불필요
        def recompute(row):
            status = infer_status(row)
            row = row.copy()
            row['_status'] = status  # infer_status 내부에서 _status 참조 안 하므로 안전
            delay = calc_delay_days(row)
            diff = calc_stage_diff(row)
            return pd.Series({
                '_status': status,
                '_delay_days': delay,
                '_cur_diff': diff.get('cur_diff'),
                '_cur_has_actual': diff.get('cur_has_actual', False),
                '_next_diff': diff.get('next_diff'),
            })
        refreshed = df.apply(recompute, axis=1)
        for col in refreshed.columns:
            df[col] = refreshed[col]
        return df

    def _get_fresh_df(self, product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "") -> pd.DataFrame:
        """필터 적용 + 날짜 재계산된 df 반환."""
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
        df = self._refresh_dynamic(df)
        if date_col and (date_from or date_to):
            df = apply_date_range(df, date_col, date_from, date_to)
        return df
        df = self.df.copy()

        if search:
            mask = (
                df['수주번호'].astype(str).str.contains(search, case=False, na=False) |
                df['업체명'].astype(str).str.contains(search, case=False, na=False) |
                df['프로젝트'].astype(str).str.contains(search, case=False, na=False)
            )
            if '시스템명' in df.columns:
                mask = mask | df['시스템명'].astype(str).str.contains(search, case=False, na=False)
            if '품명' in df.columns:
                mask = mask | df['품명'].astype(str).str.contains(search, case=False, na=False)
            df = df[mask]

        if status_filter and status_filter != "전체":
            df = df[df['_status'] == status_filter]

        if company_filter and company_filter != "전체":
            df = df[df['업체명'] == company_filter]

        if step_filter and step_filter != "전체":
            df = df[df['_current_step'] == step_filter]

        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]

        return df

    def get_processes(self, page=1, page_size=50, search="", status_filter="", company_filter="", step_filter="", sort_by="수주번호", sort_dir="asc", product_filter="", date_col="요구납기일", date_from="", date_to="") -> Dict:
        df = self.get_filtered_df(search, status_filter, company_filter, step_filter, product_filter)
        if (date_from or date_to):
            df = apply_date_range(df, date_col, date_from, date_to)

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
        if not mask.any():
            return False

        for col, val in updates.items():
            if col not in self.df.columns:
                self.df[col] = None  # 컬럼 없으면 신규 추가
            self.df.loc[mask, col] = val

        for idx in self.df[mask].index:
            row = self.df.loc[idx]
            self.df.at[idx, '_current_step'] = infer_current_step(row)
            # _current_step 반영 후 row 재조회
            row = self.df.loc[idx]
            diff = calc_stage_diff(row)
            self.df.at[idx, '_status'] = infer_status(row)
            self.df.at[idx, '_progress'] = calc_progress(row)
            self.df.at[idx, '_delay_days'] = calc_delay_days(row)
            self.df.at[idx, '_cur_diff'] = diff.get('cur_diff')
            self.df.at[idx, '_cur_has_actual'] = diff.get('cur_has_actual', False)
            self.df.at[idx, '_next_diff'] = diff.get('next_diff')

        try:
            save_df = self.df.drop(columns=[c for c in self.df.columns if c.startswith('_')], errors='ignore')
            save_df.to_excel(self.filepath, index=False)
        except Exception as e:
            print(f"[DataManager] Save error: {e}")

        return True

    def get_kpi(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "") -> Dict:
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to)
        total = len(df)
        on_track = len(df[df['_status'] == 'On Track'])
        at_risk = len(df[df['_status'] == 'At Risk'])
        delayed = len(df[df['_status'] == '지연'])
        completed = len(df[df['_status'] == '완료'])
        avg_progress = int(df['_progress'].mean()) if total > 0 else 0

        # 시스템명별 전체/완료 건수
        system_counts = {}
        system_completed = {}
        if '시스템명' in df.columns:
            for sys, grp in df.groupby('시스템명'):
                system_counts[str(sys)] = len(grp)
                system_completed[str(sys)] = len(grp[grp['_status'] == '완료'])

        in_progress = on_track + at_risk + delayed
        return {"total": total, "in_progress": in_progress, "on_track": on_track,
                "at_risk": at_risk, "delayed": delayed,
                "completed": completed, "avg_progress": avg_progress,
                "system_counts": system_counts, "system_completed": system_completed}

    def get_process_load(self, product_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]

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
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
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

    def get_alerts(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "") -> Dict:
        if self.df.empty:
            return {"delayed": [], "at_risk": [], "due_soon": {"출고": [], "OTP": []}}

        df = self._get_fresh_df(product_filter, date_col, date_from, date_to)
        today = pd.Timestamp.now()
        this_month_start = today.replace(day=1)
        next_month_start = this_month_start + pd.DateOffset(months=1)

        def row_summary(row):
            return {
                "수주번호": row.get('수주번호', ''),
                "ordseq": int(row.get('ordseq', 0)),
                "업체명": row.get('업체명', ''),
                "프로젝트": row.get('프로젝트', ''),
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

        due_soon_생산 = []
        if '생산예상일' in df.columns:
            mask = df['생산예상일'].notna() & (df['생산예상일'] >= this_month_start) & (df['생산예상일'] < next_month_start) & (df['_status'] != '완료')
            due_soon_생산 = [row_summary(row) for _, row in df[mask].iterrows()]

        return {"delayed": delayed, "at_risk": at_risk, "due_soon": {"출고": due_soon_출고, "OTP": due_soon_otp, "생산": due_soon_생산}}

    def get_company_distribution(self, product_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
        counts = df['업체명'].value_counts().head(10)
        return [{"name": k, "value": int(v)} for k, v in counts.items()]

    def get_urgent_delays(self, limit: int = 5, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "") -> List[Dict]:
        """지연 TOP5: 단계별 지연일수 기준 정렬"""
        df = self._get_fresh_df(product_filter)  # 날짜 필터 미적용 — 전체 지연 건 대상
        today = pd.Timestamp.now()
        delayed = df[df['_status'] == '지연'].copy()
        delayed = delayed[delayed['_delay_days'] > 0]
        delayed = delayed.sort_values('_delay_days', ascending=False).head(limit)
        result = []
        for _, row in delayed.iterrows():
            result.append({
                "수주번호": row.get('수주번호', ''),
                "ordseq": int(row.get('ordseq', 0)),
                "업체명": row.get('업체명', ''),
                "프로젝트": row.get('프로젝트', ''),
                "시스템명": row.get('시스템명', ''),
                "_current_step": row.get('_current_step', ''),
                "_delay_days": int(row.get('_delay_days', 0)),
                "_progress": int(row.get('_progress', 0)),
                "요구납기일": safe_date(row.get('요구납기일')),
            })
        return result

    def get_stage_by_process(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "") -> List[Dict]:
        """공정 단계별 현재 건수 (누적 바차트용)"""
        if self.df.empty:
            return []
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to)
        total_count = len(df)
        systems = sorted(df['시스템명'].dropna().unique().tolist()) if '시스템명' in df.columns else []
        system_colors = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6','#06b6d4','#f97316','#ec4899']

        result = []
        for step in PROCESS_STEPS:
            step_df = df[df['_current_step'] == step]
            step_count = len(step_df)
            by_system = []
            for si, system in enumerate(systems):
                sys_step_df = step_df[step_df['시스템명'] == system] if '시스템명' in step_df.columns else step_df
                count = len(sys_step_df)
                pct = round(count / total_count * 100) if total_count > 0 else 0
                by_system.append({
                    "system": str(system), "count": count, "pct": pct,
                    "color": system_colors[si % len(system_colors)]
                })
            # 단계별 평균 지연: 현재 이 단계 + 이미 통과한 건 모두 포함
            # actual_col이 있으면 실적일-예상일, 없고 현재 단계면 오늘-예상일
            avg_delay = None
            step_map = STEP_DATE_MAP.get(step, {})
            planned_col = step_map.get('planned')
            actual_col  = step_map.get('actual')
            if planned_col and planned_col in df.columns:
                today = pd.Timestamp.now()
                diffs = []
                # 이 단계를 거쳤거나 현재 이 단계인 모든 건
                if actual_col and actual_col in df.columns:
                    passed_df = df[df[actual_col].notna()]  # 이미 통과한 건
                else:
                    passed_df = pd.DataFrame()
                candidate_df = pd.concat([step_df, passed_df]).drop_duplicates()
                for _, r in candidate_df.iterrows():
                    planned = r.get(planned_col)
                    if planned is None or pd.isna(planned):
                        continue
                    actual = r.get(actual_col) if actual_col else None
                    if actual is not None and pd.notna(actual):
                        diff = (pd.Timestamp(actual) - pd.Timestamp(planned)).days
                    elif r.get('_current_step') == step:
                        diff = (today - pd.Timestamp(planned)).days
                    else:
                        continue  # 통과했고 실적도 없으면 집계 제외
                    if diff > 0:
                        diffs.append(diff)
                avg_delay = round(sum(diffs) / len(diffs)) if diffs else 0
            result.append({
                "step": step,
                "total": total_count,
                "project_count": step_count,
                "pct": round(step_count / total_count * 100) if total_count > 0 else 0,
                "by_system": by_system,
                "avg_delay_days": avg_delay,
            })
        return result

    def get_status_distribution(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "") -> Dict:
        """전체 상태 분포 (파이차트용)"""
        if self.df.empty:
            return {}
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to)
        total = len(df)
        return {
            "total": total,
            "on_track": int(len(df[df['_status'] == 'On Track'])),
            "at_risk": int(len(df[df['_status'] == 'At Risk'])),
            "delayed": int(len(df[df['_status'] == '지연'])),
            "completed": int(len(df[df['_status'] == '완료'])),
        }

    def get_monthly_delivery(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "") -> List[Dict]:
        """월별 출고예정(date_col) + 납품완료(최종납기일) 건수 및 상세"""
        if self.df.empty or date_col not in self.df.columns:
            if '요구납기일' not in self.df.columns:
                return []
            date_col = '요구납기일'
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]

        def row_brief(row):
            return {
                "수주번호": row.get('수주번호', ''),
                "ordseq": int(row.get('ordseq', 0)),
                "업체명": row.get('업체명', ''),
                "프로젝트": row.get('프로젝트', ''),
                "시스템명": row.get('시스템명', ''),
                "_current_step": row.get('_current_step', ''),
                "_progress": int(row.get('_progress', 0)),
                "요구납기일": safe_date(row.get('요구납기일')),
                "최종납기일": safe_date(row.get('최종납기일')),
            }

        # 출고예정: date_col 기준 + 날짜 범위
        df_filtered = apply_date_range(df, date_col, date_from, date_to) if (date_from or date_to) else df
        planned_df = df_filtered[df_filtered[date_col].notna()].copy()
        planned_df['month'] = planned_df[date_col].dt.to_period('M').astype(str)

        # 납품완료: 최종납기일 기준
        completed_df = pd.DataFrame()
        if '최종납기일' in df.columns:
            completed_df = df[df['최종납기일'].notna()].copy()
            completed_df['month'] = completed_df['최종납기일'].dt.to_period('M').astype(str)

        months = set(planned_df['month'].tolist())
        if not completed_df.empty:
            months |= set(completed_df['month'].tolist())

        result = []
        for month in sorted(months):
            planned_rows = planned_df[planned_df['month'] == month]
            completed_rows = completed_df[completed_df['month'] == month] if not completed_df.empty else pd.DataFrame()
            result.append({
                'month': month,
                'count': len(planned_rows),
                'completed': len(completed_rows),
                'planned_items': [row_brief(r) for _, r in planned_rows.iterrows()],
                'completed_items': [row_brief(r) for _, r in completed_rows.iterrows()],
            })
        result.sort(key=lambda x: x['month'])
        return result[-12:]
    def get_unique_values(self, col: str) -> List[str]:
        if col not in self.df.columns:
            return []
        vals = self.df[col].dropna().unique().tolist()
        return sorted([str(v) for v in vals])
