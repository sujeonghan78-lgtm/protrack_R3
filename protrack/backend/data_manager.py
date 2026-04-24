import pandas as pd
import numpy as np
from datetime import datetime, date
import math
from typing import Optional, Dict, Any, List


PROCESS_STEPS = ['수주', '시방', '자재', '생산', '검사', '포장', '출고', 'OTP', '계산서']

STEP_DATE_MAP = {
    '수주':  {'planned': None,              'actual': '수주일자'},
    '시방':  {'planned': '시방예상일',      'actual': '시방출도일'},
    '자재':  {'planned': None,             'actual': '자재입고일'},
    '생산':  {'planned': '생산예상일',      'actual': '생산완료일'},
    '검사':  {'planned': '검사예상일',      'actual': '품질검사일'},
    '포장':  {'planned': '포장완료예정일',  'actual': '포장완료일'},
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
    """마지막으로 실적이 찍힌 단계를 현재 단계로 반환 (Stage Progress 표시용)."""
    is_domestic = row.get('_vendor_type') == '국내'
    if pd.notna(row.get('계산서발행일')): return '계산서'
    if not is_domestic and pd.notna(row.get('OTP일자')): return 'OTP'
    if pd.notna(row.get('최종납기일')):   return '출고'
    if pd.notna(row.get('포장완료일')):   return '포장'
    if pd.notna(row.get('품질검사일')):   return '검사'
    if pd.notna(row.get('생산완료일')):   return '생산'
    if pd.notna(row.get('자재입고일')):   return '자재'
    if pd.notna(row.get('시방출도일')):   return '시방'
    if pd.notna(row.get('수주일자')):     return '수주'
    return '수주'


def infer_next_pending_step(row) -> str:
    """지연 판단용 — 실적이 없는 첫 번째 단계 반환.
    자재/검사는 데이터 미운용이므로 스킵. 국내 건은 OTP도 스킵."""
    is_domestic = row.get('_vendor_type') == '국내'
    SKIP_STEPS = {'자재', '검사'}
    if is_domestic:
        SKIP_STEPS = SKIP_STEPS | {'OTP'}
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
            continue
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
    """이전공정 실적일 + 다음단계 예정일(planned 있는 첫 단계) 반환"""
    today = pd.Timestamp.now()
    is_domestic = row.get('_vendor_type') == '국내'
    SKIP_STEPS = {'자재', '검사'}
    if is_domestic:
        SKIP_STEPS = SKIP_STEPS | {'OTP'}

    current_step = infer_next_pending_step(row)
    steps = list(STEP_DATE_MAP.keys())
    cur_idx = steps.index(current_step) if current_step in steps else -1

    # ── 이전공정 실적일: current_step 바로 이전 단계의 actual ──
    cur_actual = None
    for i in range(cur_idx - 1, -1, -1):
        prev_step = steps[i]
        if prev_step in SKIP_STEPS:
            continue
        prev_actual_col = STEP_DATE_MAP.get(prev_step, {}).get('actual')
        if prev_actual_col:
            val = row.get(prev_actual_col)
            if val is not None and pd.notna(val):
                cur_actual = val
                break

    # ── 현재 단계 planned (지연 계산용) ──
    cur_map = STEP_DATE_MAP.get(current_step, {})
    cur_planned_col = cur_map.get('planned')
    cur_planned = row.get(cur_planned_col) if cur_planned_col else None

    # ── 다음단계 예정일: planned 있는 첫 번째 다음 단계 ──
    next_planned = None
    for i in range(cur_idx + 1, len(steps)):
        next_step = steps[i]
        if next_step in SKIP_STEPS:
            continue
        next_planned_col = STEP_DATE_MAP.get(next_step, {}).get('planned')
        if next_planned_col:
            val = row.get(next_planned_col)
            if val is not None and pd.notna(val):
                next_planned = val
                break

    return {
        'cur_actual':   cur_actual  if cur_actual  is not None and pd.notna(cur_actual)  else None,
        'cur_planned':  cur_planned if cur_planned is not None and pd.notna(cur_planned) else None,
        'next_planned': next_planned if next_planned is not None and pd.notna(next_planned) else None,
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
    """완료/지연 분기:
    - 계산서발행일 있음 → 계산서완료 or 계산서지연
    - OTP실적 있고 최종납기일 없음 → 데이터오류
    - 최종납기일 있음 → 출고완료 or 출고지연 or OTP지연 or 계산서지연
    - 나머지 → 공정 중 지연/임박/정상
    """
    today = pd.Timestamp.now()
    is_domestic = row.get('_vendor_type') == '국내'

    # ── 계산서 발행 완료 ──────────────────────────────
    if pd.notna(row.get('계산서발행일')):
        invoice_date = pd.Timestamp(row['계산서발행일'])
        if is_domestic:
            # 국내: 계산서 발행월 > 출고월이면 지연
            if pd.notna(row.get('최종납기일')):
                출고월 = pd.Timestamp(row['최종납기일']).to_period('M')
                계산서월 = invoice_date.to_period('M')
                if 계산서월 > 출고월:
                    return '계산서지연'
        else:
            # 해외: 계산서 발행월 > OTP실적월이면 지연
            if pd.notna(row.get('OTP일자')):
                otp월 = pd.Timestamp(row['OTP일자']).to_period('M')
                계산서월 = invoice_date.to_period('M')
                if 계산서월 > otp월:
                    return '계산서지연'
        return '계산서완료'

    # ── 데이터 오류: OTP실적 있는데 최종납기일 없음 ──
    if not is_domestic and pd.notna(row.get('OTP일자')) and pd.isna(row.get('최종납기일')):
        return '데이터오류'

    # ── 출고 완료 이후 단계 ───────────────────────────
    if pd.notna(row.get('최종납기일')):
        출고일 = pd.Timestamp(row['최종납기일'])
        요구납기일 = row.get('요구납기일')

        if not is_domestic:
            # 해외: OTP 지연 체크
            if pd.notna(row.get('OTP일자')) and pd.notna(row.get('OTP예상일')):
                if pd.Timestamp(row['OTP일자']) > pd.Timestamp(row['OTP예상일']):
                    return 'OTP지연'
            # OTP 미완료 상태면 아직 진행중 — 출고완료로 보지 않음
            # (OTP예상일 초과 여부는 공정 중 지연으로 처리)

        # 출고 지연: 최종납기일 > 요구납기일
        if pd.notna(요구납기일):
            if 출고일 > pd.Timestamp(요구납기일):
                return '출고지연'
        return '출고완료'

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
    current_step = infer_next_pending_step(row)

    # 완료 건은 0
    if row.get('_status') in ('출고완료', '계산서완료') or pd.notna(row.get('계산서발행일')):
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

        # 거래처 구분 적용
        vendors = self._load_vendors()
        def get_vendor_type(name):
            if pd.isna(name): return '미분류'
            return vendors.get(str(name).strip(), '미분류')
        df['_vendor_type'] = df['업체명'].apply(get_vendor_type)

        def enrich_row(row):
            info = get_current_next_step_info(row)
            diff = calc_stage_diff(row)
            cur_actual = info.get('cur_actual')
            next_planned = info.get('next_planned')
            return pd.Series({
                '_status': infer_status(row),
                '_progress': calc_progress(row),
                '_delay_days': calc_delay_days(row),
                '_cur_diff': diff.get('cur_diff'),
                '_cur_has_actual': diff.get('cur_has_actual', False),
                '_next_diff': diff.get('next_diff'),
                '_cur_actual_date': pd.Timestamp(cur_actual).strftime('%Y-%m-%d') if cur_actual is not None and pd.notna(cur_actual) else None,
                '_next_planned_date': pd.Timestamp(next_planned).strftime('%Y-%m-%d') if next_planned is not None and pd.notna(next_planned) else None,
            })

        enriched = df.apply(enrich_row, axis=1)
        df = pd.concat([df, enriched], axis=1)
        df['_row_id'] = df.index
        return df

    def _load_vendors(self) -> dict:
        import json, os
        vendors_file = os.path.join(os.path.dirname(self.filepath), 'vendors.json')
        if not os.path.exists(vendors_file):
            return {}
        try:
            with open(vendors_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}

    def reload_vendors(self):
        """거래처 파일 변경 시 _vendor_type 재계산"""
        if self.df.empty: return
        vendors = self._load_vendors()
        def get_vendor_type(name):
            if pd.isna(name): return '미분류'
            return vendors.get(str(name).strip(), '미분류')
        self.df['_vendor_type'] = self.df['업체명'].apply(get_vendor_type)

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

    def get_filtered_df(self, search="", status_filter="", company_filter="", step_filter="", product_filter="", vendor_filter="") -> pd.DataFrame:
        df = self.df.copy()

        if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
            df = df[df['_vendor_type'] == vendor_filter]

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
            if status_filter == "지연":
                df = df[df['_status'].isin(['지연', '출고지연', 'OTP지연', '계산서지연'])]
            else:
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

    def _refresh_dynamic(self, df: pd.DataFrame) -> pd.DataFrame:
        """조회 시점의 오늘 날짜로 상태·지연일수를 재계산."""
        df = df.copy()
        # _current_step은 실적 기반이라 날짜 무관 — 재계산 불필요
        def recompute(row):
            status = infer_status(row)
            row = row.copy()
            row['_status'] = status
            delay = calc_delay_days(row)
            info = get_current_next_step_info(row)
            diff = calc_stage_diff(row)
            cur_actual = info.get('cur_actual')
            next_planned = info.get('next_planned')
            return pd.Series({
                '_status': status,
                '_delay_days': delay,
                '_cur_diff': diff.get('cur_diff'),
                '_cur_has_actual': diff.get('cur_has_actual', False),
                '_next_diff': diff.get('next_diff'),
                '_cur_actual_date': pd.Timestamp(cur_actual).strftime('%Y-%m-%d') if cur_actual is not None and pd.notna(cur_actual) else None,
                '_next_planned_date': pd.Timestamp(next_planned).strftime('%Y-%m-%d') if next_planned is not None and pd.notna(next_planned) else None,
            })
        refreshed = df.apply(recompute, axis=1)
        for col in refreshed.columns:
            df[col] = refreshed[col]
        return df

    def _get_fresh_df(self, product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> pd.DataFrame:
        """필터 적용 + 날짜 재계산된 df 반환."""
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
        if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
            df = df[df['_vendor_type'] == vendor_filter]
        df = self._refresh_dynamic(df)
        if date_col and (date_from or date_to):
            df = apply_date_range(df, date_col, date_from, date_to)
        return df

    def get_processes(self, page=1, page_size=50, search="", status_filter="", company_filter="", step_filter="", sort_by="수주번호", sort_dir="asc", product_filter="", date_col="요구납기일", date_from="", date_to="", vendor_filter="") -> Dict:
        df = self.get_filtered_df(search, status_filter, company_filter, step_filter, product_filter, vendor_filter)
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
        is_domestic = row.get('_vendor_type') == '국내'
        for step in PROCESS_STEPS:
            if is_domestic and step == 'OTP':
                continue
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
            return False

        return True

    def get_kpi(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> Dict:
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)
        total = len(df)
        on_track  = len(df[df['_status'] == 'On Track'])
        at_risk   = len(df[df['_status'] == 'At Risk'])

        # 지연 분류
        delayed_process  = len(df[df['_status'] == '지연'])          # 공정 중 지연
        delayed_delivery = len(df[df['_status'] == '출고지연'])       # 출고 지연
        delayed_post     = len(df[df['_status'].isin(['OTP지연', '계산서지연'])])  # 출고 이후 지연
        delayed_total    = delayed_process + delayed_delivery + delayed_post

        completed = len(df[df['_status'].isin(['출고완료', '계산서완료'])])
        delivered = len(df[df['_status'] == '출고완료'])
        invoiced  = len(df[df['_status'] == '계산서완료'])
        data_error = len(df[df['_status'] == '데이터오류'])
        avg_progress = int(df['_progress'].mean()) if total > 0 else 0

        system_counts = {}
        system_completed = {}
        if '시스템명' in df.columns:
            for sys, grp in df.groupby('시스템명'):
                system_counts[str(sys)] = len(grp)
                system_completed[str(sys)] = len(grp[grp['_status'].isin(['출고완료', '계산서완료'])])

        in_progress = on_track + at_risk + delayed_process
        return {"total": total, "in_progress": in_progress, "on_track": on_track,
                "at_risk": at_risk,
                "delayed": delayed_total,
                "delayed_process": delayed_process,
                "delayed_delivery": delayed_delivery,
                "delayed_post": delayed_post,
                "completed": completed, "delivered": delivered, "invoiced": invoiced,
                "data_error": data_error,
                "avg_progress": avg_progress,
                "system_counts": system_counts, "system_completed": system_completed}

    def get_process_load(self, product_filter: str = "", vendor_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
        if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
            df = df[df['_vendor_type'] == vendor_filter]

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

    def get_stage_progress(self, product_filter: str = "", vendor_filter: str = "") -> List[Dict]:
        if self.df.empty:
            return []
        df = self.df.copy()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
        if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
            df = df[df['_vendor_type'] == vendor_filter]
        if '시스템명' not in df.columns:
            return []

        result = []
        for system, group in df.groupby('시스템명'):
            total = len(group)
            completed = len(group[group['_status'].isin(['출고완료', '계산서완료'])])
            rate = int(completed / total * 100) if total > 0 else 0
            result.append({"system": str(system), "total": total, "completed": completed, "rate": rate})
        result.sort(key=lambda x: x['rate'], reverse=True)
        return result

    def get_alerts(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> Dict:
        if self.df.empty:
            return {"delayed": [], "at_risk": [], "due_soon": {"출고": [], "OTP": []}, "data_error": []}

        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)
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
                "_vendor_type": row.get('_vendor_type', '미분류'),
                "요구납기일": safe_date(row.get('요구납기일')),
                "OTP예상일": safe_date(row.get('OTP예상일')),
            }

        # 7번 수정: 지연은 요구납기일이 오늘 이전인 건만 (실질 납기 초과)
        delayed_df = df[(df['_status'] == '지연') & df['요구납기일'].notna() & (df['요구납기일'] < today)]
        delayed = [row_summary(row) for _, row in delayed_df.sort_values('_delay_days', ascending=False).iterrows()]
        at_risk = [row_summary(row) for _, row in df[df['_status'] == 'At Risk'].iterrows()]

        due_soon_출고 = []
        if '요구납기일' in df.columns:
            # 8번 수정: next_month_start 경계값 제외 (날짜만 비교)
            mask = (df['요구납기일'].notna() &
                    (df['요구납기일'].dt.date >= this_month_start.date()) &
                    (df['요구납기일'].dt.date < next_month_start.date()) &
                    (~df['_status'].isin(['출고완료', '계산서완료'])))
            due_soon_출고 = [row_summary(row) for _, row in df[mask].iterrows()]

        due_soon_otp = []
        if 'OTP예상일' in df.columns:
            mask = df['OTP예상일'].notna() & (df['OTP예상일'] >= this_month_start) & (df['OTP예상일'] < next_month_start) & (~df['_status'].isin(['출고완료', '계산서완료']))
            due_soon_otp = [row_summary(row) for _, row in df[mask].iterrows()]

        due_soon_생산 = []
        if '생산예상일' in df.columns:
            mask = df['생산예상일'].notna() & (df['생산예상일'] >= this_month_start) & (df['생산예상일'] < next_month_start) & (~df['_status'].isin(['출고완료', '계산서완료', '출고지연', 'OTP지연', '계산서지연']))
            due_soon_생산 = [row_summary(row) for _, row in df[mask].iterrows()]

        # 데이터 오류: OTP실적 있는데 최종납기일 없는 건
        data_error_df = df[df['_status'] == '데이터오류']
        data_error = []
        for _, row in data_error_df.iterrows():
            item = row_summary(row)
            item['오류내용'] = 'OTP 실적 있으나 출고일 미입력'
            data_error.append(item)

        return {"delayed": delayed, "at_risk": at_risk,
                "due_soon": {"출고": due_soon_출고, "OTP": due_soon_otp, "생산": due_soon_생산},
                "data_error": data_error}

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

    def get_urgent_delays(self, limit: int = 5, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> List[Dict]:
        """지연 TOP5: 요구납기일 기준 지연일수로 정렬"""
        df = self._get_fresh_df(product_filter, vendor_filter=vendor_filter)
        today = pd.Timestamp.now()
        delayed = df[df['_status'] == '지연'].copy()

        # 요구납기일 기준 지연일수 계산
        def due_delay(row):
            due = row.get('요구납기일')
            if due is None or pd.isna(due):
                return 0
            return max(0, (today - pd.Timestamp(due)).days)

        delayed['_due_delay'] = delayed.apply(due_delay, axis=1)
        delayed = delayed[delayed['_due_delay'] > 0]
        delayed = delayed.sort_values('_due_delay', ascending=False).head(limit)

        result = []
        for _, row in delayed.iterrows():
            result.append({
                "수주번호": row.get('수주번호', ''),
                "ordseq": int(row.get('ordseq', 0)),
                "업체명": row.get('업체명', ''),
                "프로젝트": row.get('프로젝트', ''),
                "시스템명": row.get('시스템명', ''),
                "_current_step": row.get('_current_step', ''),
                "_delay_days": int(row.get('_due_delay', 0)),
                "_progress": int(row.get('_progress', 0)),
                "요구납기일": safe_date(row.get('요구납기일')),
            })
        return result

    def get_stage_by_process(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> List[Dict]:
        """공정 단계별 현재 건수 (누적 바차트용)"""
        if self.df.empty:
            return []
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)
        total_count = len(df)
        systems = sorted(df['시스템명'].dropna().unique().tolist()) if '시스템명' in df.columns else []
        system_colors = ['#2563eb','#3b82f6','#1e40af','#60a5fa','#1d4ed8','#93c5fd','#bfdbfe','#1e3a8a']

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
            # 지연 건수: 공정중 지연 + 완료 지연 모두 포함 (완료(정상) 제외)
            DELAY_STATUSES = {'지연', '출고지연', 'OTP지연', '계산서지연'}
            cur_diffs = [r['_cur_diff'] for _, r in step_df.iterrows()
                         if r.get('_cur_diff') is not None and not (isinstance(r['_cur_diff'], float) and pd.isna(r['_cur_diff'])) and r['_cur_diff'] > 0
                         and r.get('_status') not in ('출고완료', '계산서완료', '데이터오류')]
            avg_cur = round(sum(cur_diffs) / len(cur_diffs)) if cur_diffs else None
            delayed_count = len(cur_diffs)

            # 모드2: 다음 일정 초과 평균 — 전체 건 기준, 미초과=0 포함
            next_diffs = [max(0, r['_next_diff']) for _, r in step_df.iterrows()
                          if r.get('_next_diff') is not None and not (isinstance(r['_next_diff'], float) and pd.isna(r['_next_diff']))]
            avg_next = round(sum(next_diffs) / len(next_diffs)) if next_diffs else None

            result.append({
                "step": step,
                "total": total_count,
                "project_count": step_count,
                "pct": round(step_count / total_count * 100) if total_count > 0 else 0,
                "by_system": by_system,
                "avg_delay_days": avg_cur,
                "avg_cur_days": avg_cur,
                "avg_next_days": avg_next,
                "delayed_count": delayed_count,
            })
        return result

    def get_status_distribution(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> Dict:
        """전체 상태 분포 (파이차트용)"""
        if self.df.empty:
            return {}
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)
        total = len(df)
        return {
            "total":        total,
            "on_track":     int(len(df[df['_status'] == 'On Track'])),
            "at_risk":      int(len(df[df['_status'] == 'At Risk'])),
            "delayed":      int(len(df[df['_status'] == '지연'])),
            "delivered":    int(len(df[df['_status'] == '출고완료'])),
            "delivered_delayed": int(len(df[df['_status'] == '출고지연'])),
            "invoiced":     int(len(df[df['_status'] == '계산서완료'])),
            "invoiced_delayed": int(len(df[df['_status'] == '계산서지연'])),
            "otp_normal":   int(len(df[df['_status'].isin(['출고완료','출고지연','OTP지연','계산서완료','계산서지연']) & df['OTP일자'].notna()])),
            "otp_delayed":  int(len(df[df['_status'] == 'OTP지연'])),
            "delayed_delivery": int(len(df[df['_status'] == '출고지연'])),
            "delayed_post": int(len(df[df['_status'].isin(['OTP지연', '계산서지연'])])),
            "data_error":   int(len(df[df['_status'] == '데이터오류'])),
        }

    def get_monthly_delivery(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> List[Dict]:
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
        if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
            df = df[df['_vendor_type'] == vendor_filter]

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

        # 출고예정: date_col 기준 + 날짜 범위 + 아직 출고 안 된 건만 (중복 제거)
        df_filtered = apply_date_range(df, date_col, date_from, date_to) if (date_from or date_to) else df
        planned_df = df_filtered[df_filtered[date_col].notna() & df_filtered['최종납기일'].isna()].copy()
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


    def get_summary(self) -> Dict:
        """상단 바 파일 정보 패널용 요약"""
        if self.df.empty:
            return {}
        df = self.df.copy()

        systems = sorted(df['시스템명'].dropna().unique().tolist()) if '시스템명' in df.columns else []

        due_min = due_max = None
        if '요구납기일' in df.columns:
            due_series = df['요구납기일'].dropna()
            if not due_series.empty:
                due_min = safe_date(due_series.min())
                due_max = safe_date(due_series.max())

        domestic_companies = set()
        overseas_companies = set()
        unclassified_companies = set()
        if '_vendor_type' in df.columns and '업체명' in df.columns:
            for _, row in df[['업체명', '_vendor_type']].drop_duplicates().iterrows():
                name = str(row['업체명']).strip()
                vtype = str(row['_vendor_type'])
                if vtype == '국내':
                    domestic_companies.add(name)
                elif vtype == '해외':
                    overseas_companies.add(name)
                else:
                    unclassified_companies.add(name)

        step_counts = {}
        if '_current_step' in df.columns:
            for step in PROCESS_STEPS:
                cnt = int(len(df[df['_current_step'] == step]))
                if cnt > 0:
                    step_counts[step] = cnt

        return {
            "total": len(df),
            "systems": [str(s) for s in systems],
            "due_min": due_min,
            "due_max": due_max,
            "vendor_counts": {
                "국내": len(domestic_companies),
                "해외": len(overseas_companies),
                "미분류": len(unclassified_companies),
            },
            "step_counts": step_counts,
        }
