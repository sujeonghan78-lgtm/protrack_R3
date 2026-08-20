import pandas as pd
import numpy as np
import unicodedata
from datetime import datetime, date
import math
import time
from typing import Optional, Dict, Any, List


PROCESS_STEPS = ['수주', '시방', '자재', '생산', '검사', '포장', '출고', 'OTP', '계산서']

STEP_DATE_MAP = {
    '수주':  {'planned': None,              'actual': '수주일자'},
    '시방':  {'planned': '시방예상일',      'actual': '시방출도일'},
    '자재':  {'planned': '자재예상일',      'actual': '자재입고일'},
    '생산':  {'planned': '생산예상일',      'actual': '생산완료일'},
    '검사':  {'planned': '품질검사예상일',  'actual': '품질검사일'},
    '포장':  {'planned': '포장완료예상일',  'actual': '포장완료일'},
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

# [BOM개편] 마일스톤 바용 — PROCESS_STEPS 각 단계가 calc_progress의 0~100% 스케일에서
# 차지하는 폭. PROGRESS_WEIGHTS는 자재를 '자재확인일'+'자재입고일' 두 컬럼(10+10)으로
# 나눠서 세지만, 여기선 '자재' 한 단계로 묶어 20으로 합산 — PROCESS_STEPS 9단계와 1:1 대응시키기 위함.
STEP_WIDTH = {
    '수주': 5, '시방': 10, '자재': 20, '생산': 20, '검사': 10,
    '포장': 10, '출고': 10, 'OTP': 10, '계산서': 5,
}
STEP_CUM_END = {}
_cum = 0
for _s in PROCESS_STEPS:
    _cum += STEP_WIDTH[_s]
    STEP_CUM_END[_s] = _cum
# 출고 단계가 끝나는 지점(=수주~출고 누적 비중) — 마일스톤 점 고정 위치로 사용
SHIP_MILESTONE_PCT = STEP_CUM_END['출고']  # 85

# "출고까지 실제로 끝났다"고 볼 수 있는 상태 — 출고지연(아직 실제 출고 안 됨)은 제외
SHIPPED_STATUSES = {'출고완료', '계산서완료', 'OTP지연', '계산서지연'}

# 지연으로 간주하는 _status 값 (여러 곳에서 중복 정의되어 있던 걸 재사용 가능하도록 통합)
DELAY_STATUSES = {'지연', '출고지연', 'OTP지연', '계산서지연'}

# 자재 단계 "STOCK"(스탁자재=이미 재고 확보) 처리용 컬럼.
# STOCK 표시는 그대로 유지하되, 진행/진척 판단에서는 완료로 인정하고
# 지연 판정 대상에서는 제외한다(=현재단계가 더는 자재에 머물지 않게 해서 자연스럽게 제외됨).
MATERIAL_ACTUAL_COL = '자재입고일'
MATERIAL_PLANNED_COL = '자재예상일'
MATERIAL_ACTUAL_STOCK_COL = 'mat_actual_stock'   # underscore 없이 저장 — reload_vendors()가 '_' 컬럼을 지우기 때문
MATERIAL_PLANNED_STOCK_COL = 'mat_planned_stock'
MATERIAL_STOCK_COL = 'mat_stock'  # 위 둘 중 하나라도 True면 True


def is_material_stock(row) -> bool:
    return bool(row.get(MATERIAL_STOCK_COL, False))


def safe_date(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    if isinstance(val, (datetime, date, pd.Timestamp)):
        try:
            return pd.Timestamp(val).strftime('%Y-%m-%d')
        except:
            return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        # 8자리 숫자 문자열(예: '20260812')도 처리
        if s.isdigit() and len(s) == 8:
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        try:
            ts = pd.Timestamp(s)
            if pd.notna(ts):
                return ts.strftime('%Y-%m-%d')
        except:
            return None
        return None
    if isinstance(val, (int, float)):
        try:
            s = str(int(val))
            if len(s) == 8:
                return f"{s[:4]}-{s[4:6]}-{s[6:]}"
        except:
            pass
    return None


def get_effective_steps(row) -> list:
    """차수(국내/수출)에 따라 실제로 진행되는 단계 목록.
    국내 건은 포장/OTP 프로세스 자체가 없으므로 제외. 자재/검사는 스킵하지 않음."""
    is_domestic = row.get('_vendor_type') == '국내'
    if is_domestic:
        return [s for s in PROCESS_STEPS if s not in ('포장', 'OTP')]
    return list(PROCESS_STEPS)


def infer_current_step(row) -> str:
    """실적(actual)이 찍힌 단계 중 가장 마지막(뒤) 단계를 찾아, 그 다음 단계를 현재 단계로 반환.
    (Stage Progress 표시/필터/KPI/지연계산 공용)
    중간에 실적이 비어있는 단계가 있어도(예: 자재 데이터 누락) 더 뒤 단계에 실적이 있으면
    그 뒤 단계까지는 완료된 것으로 보고 건너뛴다.
    자재 단계는 STOCK(재고 이미 확보)이면 실적 유무와 무관하게 완료로 간주한다."""
    steps = get_effective_steps(row)
    last_actual_idx = -1
    for i, step in enumerate(steps):
        actual_col = STEP_DATE_MAP.get(step, {}).get('actual')
        has_actual = bool(actual_col and pd.notna(row.get(actual_col)))
        if step == '자재' and is_material_stock(row):
            has_actual = True
        if has_actual:
            last_actual_idx = i
    if last_actual_idx == len(steps) - 1:
        return steps[-1] if steps else '수주'
    return steps[last_actual_idx + 1] if steps else '수주'


def calc_progress(row) -> int:
    weight_cols = list(PROGRESS_WEIGHTS.keys())
    last_idx = -1
    for i, col in enumerate(weight_cols):
        has_data = pd.notna(row.get(col))
        if col == MATERIAL_ACTUAL_COL and is_material_stock(row):
            has_data = True
        if has_data:
            last_idx = i
    if last_idx < 0:
        return 0
    total = sum(PROGRESS_WEIGHTS[col] for col in weight_cols[:last_idx + 1])
    return min(100, total)


def get_current_next_step_info(row, current_step=None):
    """지연 판단용 — 현재(미완료) 단계와 다음 단계의 예상/실적일 반환 (원래 로직).
    current_step을 미리 알고 있으면(예: df에 이미 _current_step 컬럼이 있으면) 넘겨서
    infer_current_step 재계산을 생략할 수 있다(성능 최적화)."""
    today = pd.Timestamp.now()
    if current_step is None:
        current_step = infer_current_step(row)

    cur_map = STEP_DATE_MAP.get(current_step, {})
    cur_actual_col  = cur_map.get('actual')
    cur_planned_col = cur_map.get('planned')
    cur_actual  = row.get(cur_actual_col)  if cur_actual_col  else None
    cur_planned = row.get(cur_planned_col) if cur_planned_col else None

    steps = get_effective_steps(row)
    cur_idx   = steps.index(current_step) if current_step in steps else -1
    next_step = steps[cur_idx + 1] if cur_idx >= 0 and cur_idx + 1 < len(steps) else None
    next_map  = STEP_DATE_MAP.get(next_step, {}) if next_step else {}
    next_planned_col = next_map.get('planned')
    next_planned = row.get(next_planned_col) if next_planned_col else None

    return {
        'cur_actual':   cur_actual  if cur_actual  is not None and pd.notna(cur_actual)  else None,
        'cur_planned':  cur_planned if cur_planned is not None and pd.notna(cur_planned) else None,
        'next_planned': next_planned if next_planned is not None and pd.notna(next_planned) else None,
        'today': today,
    }


def get_display_dates(row, status: str = None, current_step: str = None) -> dict:
    """공정 목록 화면 표시용 — 이전공정 실적일 + 현재단계 예정일.
    current_step을 미리 알고 있으면 넘겨서 재계산을 생략(성능 최적화)."""
    steps = get_effective_steps(row)

    # 현재 단계 = 실적이 찍힌 마지막 단계의 다음 단계
    if current_step is None:
        current_step = infer_current_step(row)
    cur_idx = steps.index(current_step) if current_step in steps else -1

    # 이전공정 실적일 = current_step 바로 이전 단계의 actual
    # (current_step 이전 단계는 정의상 실적이 이미 채워져 있음)
    prev_actual_date = None
    prev_idx = cur_idx - 1
    while prev_idx >= 0:
        prev_step = steps[prev_idx]
        if prev_step == '자재' and is_material_stock(row):
            prev_actual_date = 'STOCK'
            break
        prev_actual_col = STEP_DATE_MAP.get(prev_step, {}).get('actual')
        if prev_actual_col:
            val = row.get(prev_actual_col)
            if val is not None and pd.notna(val):
                prev_actual_date = pd.Timestamp(val).strftime('%Y-%m-%d')
                break
        prev_idx -= 1

    # 모든 단계가 완료된 경우(current_step이 마지막 단계=계산서, 자기 실적도 있음) 자기 자신의 실적일 사용
    cur_actual_col = STEP_DATE_MAP.get(current_step, {}).get('actual')
    is_fully_done = bool(cur_actual_col) and pd.notna(row.get(cur_actual_col))
    if prev_actual_date is None and cur_idx == len(steps) - 1 and is_fully_done:
        val = row.get(cur_actual_col)
        prev_actual_date = pd.Timestamp(val).strftime('%Y-%m-%d')

    # 현재단계 예정일 = current_step 자신의 planned. 없으면 요구납기일로 폴백.
    # 단, 모든 단계 완료 건은 예정일을 보여줄 필요 없음
    current_planned_date = None
    if not is_fully_done:
        cur_planned_col = STEP_DATE_MAP.get(current_step, {}).get('planned')
        if cur_planned_col:
            val = row.get(cur_planned_col)
            if val is not None and pd.notna(val):
                current_planned_date = pd.Timestamp(val).strftime('%Y-%m-%d')
        if current_planned_date is None:
            due = row.get('요구납기일')
            if due is not None and pd.notna(due):
                current_planned_date = pd.Timestamp(due).strftime('%Y-%m-%d')

    return {'prev_actual_date': prev_actual_date, 'current_planned_date': current_planned_date}


def calc_stage_diff(row, current_step=None) -> dict:
    """현재/다음 단계 날짜 차이 계산.
    planned 없는 단계(수주/포장/계산서)는 요구납기일로 fallback."""
    info = get_current_next_step_info(row, current_step=current_step)
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
    - 계산서발행일 있음 → 계산서완료 (실적만 찍히면 무조건 완료로 처리, 발행월 비교는 안 함)
    - OTP실적 있고 최종납기일 없음 → 데이터오류
    - 최종납기일 있음 → 출고완료(실적만 있으면 지연 무관) or OTP지연
    - 나머지 → 공정 중 지연/임박/정상
    """
    today = pd.Timestamp.now()
    is_domestic = row.get('_vendor_type') == '국내'

    # ── 계산서 발행 완료 — 실적(계산서발행일)만 찍히면 무조건 완료 ──
    if pd.notna(row.get('계산서발행일')):
        return '계산서완료'

    # ── 데이터 오류: OTP실적 있는데 최종납기일 없음 ──
    if not is_domestic and pd.notna(row.get('OTP일자')) and pd.isna(row.get('최종납기일')):
        return '데이터오류'

    # ── 출고 완료 이후 단계 ───────────────────────────
    if pd.notna(row.get('최종납기일')):
        if not is_domestic:
            # 해외: OTP 지연 체크
            if pd.notna(row.get('OTP일자')) and pd.notna(row.get('OTP예상일')):
                if pd.Timestamp(row['OTP일자']) > pd.Timestamp(row['OTP예상일']):
                    return 'OTP지연'
            # OTP 미완료 상태면 아직 진행중 — 출고완료로 보지 않음
            # (OTP예상일 초과 여부는 공정 중 지연으로 처리)

        # 출고 실적(최종납기일)이 있으면 요구납기일 대비 늦었어도 지연으로 보지 않음
        # (실적이 찍힌 시점에서 이미 출고가 끝난 것으로 판단)
        return '출고완료'

    # ── 요구납기일 초과(최우선): 출고 실적 없고 요구납기일이 오늘보다 지났으면
    #    공정 단계 상황과 무관하게 무조건 지연 처리. 단, 라벨은 현재 단계로 구분:
    #    현재 단계가 출고면 '출고지연', 그 외(검사/생산 등)는 '지연'(공정 중 지연) ──
    요구납기일 = row.get('요구납기일')
    if pd.notna(요구납기일) and today > pd.Timestamp(요구납기일):
        return '출고지연' if row.get('_current_step') == '출고' else '지연'

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


def calc_delay_days(row, current_step=None) -> int:
    """현재 단계 기준 지연일수.
    실적 있음: 실적일 - 예상일 (양수면 지연)
    실적 없음: 오늘 - 예상일 (양수면 지연)
    예상일 없음: 요구납기일 기준 fallback
    current_step을 미리 알고 있으면 넘겨서 재계산 생략(성능 최적화)."""
    today = pd.Timestamp.now()
    if current_step is None:
        current_step = infer_current_step(row)

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
    CACHE_TTL_SECONDS = 600  # [BOM개편] 상태 재계산(_refresh_dynamic) 결과 캐시 유효시간(10분)

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df: pd.DataFrame = pd.DataFrame()
        self._refreshed_cache = None
        self._refreshed_cache_at = 0.0
        self._grouped_cache = None       # [BOM개편] "필터 없는 전체" 그룹핑 결과 캐시(수주→차수→아이템)
        self._grouped_cache_at = 0.0
        self._load()

    def _invalidate_cache(self):
        """데이터가 바뀌는 시점(업로드/버전전환/수정/거래처갱신)마다 호출 — 캐시 즉시 무효화."""
        self._refreshed_cache = None
        self._refreshed_cache_at = 0.0
        self._grouped_cache = None
        self._grouped_cache_at = 0.0

    def _get_refreshed_df(self) -> pd.DataFrame:
        """self.df 전체를 _refresh_dynamic한 결과를 캐시해서 재사용.
        CACHE_TTL_SECONDS 안에 또 호출되면 재계산 없이 캐시 반환,
        지나면 이번 호출에서 한 번만 다시 계산."""
        now = time.time()
        if self._refreshed_cache is not None and (now - self._refreshed_cache_at) < self.CACHE_TTL_SECONDS:
            return self._refreshed_cache
        refreshed = self._refresh_dynamic(self.df)
        self._refreshed_cache = refreshed
        self._refreshed_cache_at = now
        return refreshed

    def _load(self):
        try:
            engine = 'xlrd' if str(self.filepath).lower().endswith('.xls') else 'openpyxl'
            df = pd.read_excel(self.filepath, engine=engine)

            # [FIX-6] 엑셀 헤더의 숨은 공백/유니코드 정규화 차이(NFC/NFD)로 인해
            # 컬럼명이 코드 상 문자열과 안 맞는 문제를 방지
            df.columns = [unicodedata.normalize('NFC', str(c)).strip() for c in df.columns]

            if '제품군' in df.columns:
                df = df[df['제품군'] != 'TLGS']
            if '시스템명' in df.columns:
                df = df[df['시스템명'] != 'TLGS']

            if 'dlvdt' in df.columns:
                df = df.rename(columns={'dlvdt': '요구납기일'})

            if 'ordseq' not in df.columns:
                df['ordseq'] = df.groupby('수주번호').cumcount() + 1

            # [BOM개편] 자재 단계 "STOCK"(스탁자재) 감지 — 날짜 파싱으로 텍스트가
            # 사라지기 전에 미리 플래그로 남겨둔다. 컬럼명에 '_' 접두어를 안 쓰는 이유는
            # reload_vendors()가 '_' 시작 컬럼을 지우고 재계산하기 때문(그때도 플래그가 남아있어야 함).
            def _is_stock_text(v):
                return isinstance(v, str) and v.strip().upper() == 'STOCK'

            if MATERIAL_ACTUAL_COL in df.columns:
                df[MATERIAL_ACTUAL_STOCK_COL] = df[MATERIAL_ACTUAL_COL].apply(_is_stock_text)
            else:
                df[MATERIAL_ACTUAL_STOCK_COL] = False
            if MATERIAL_PLANNED_COL in df.columns:
                df[MATERIAL_PLANNED_STOCK_COL] = df[MATERIAL_PLANNED_COL].apply(_is_stock_text)
            else:
                df[MATERIAL_PLANNED_STOCK_COL] = False
            df[MATERIAL_STOCK_COL] = df[MATERIAL_ACTUAL_STOCK_COL] | df[MATERIAL_PLANNED_STOCK_COL]

            # [FIX-5] fix_date를 루프 밖으로 꺼내고 to_datetime 이중변환 제거
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

            date_cols = [col for col in df.columns if '일자' in str(col) or str(col).endswith('일')]
            for col in date_cols:
                df[col] = pd.to_datetime(df[col].apply(fix_date), errors='coerce')

            self.df = self._enrich(df)
            self._invalidate_cache()
            print(f"[DataManager] Loaded {len(self.df)} rows from {self.filepath}")
        except Exception as e:
            print(f"[DataManager] Load error: {e}")
            self.df = pd.DataFrame()
            self._invalidate_cache()

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
            current_step = row.get('_current_step')
            diff = calc_stage_diff(row, current_step=current_step)
            status = infer_status(row)
            display = get_display_dates(row, status, current_step=current_step)
            return pd.Series({
                '_status': status,
                '_progress': calc_progress(row),
                '_delay_days': calc_delay_days(row, current_step=current_step),
                '_cur_diff': diff.get('cur_diff'),
                '_cur_has_actual': diff.get('cur_has_actual', False),
                '_next_diff': diff.get('next_diff'),
                '_cur_actual_date': display['prev_actual_date'],
                '_current_planned_date': display['current_planned_date'],
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
        """거래처 파일 변경 시 _vendor_type 재계산 후 파생값 전체 재계산"""
        if self.df.empty: return
        vendors = self._load_vendors()
        def get_vendor_type(name):
            if pd.isna(name): return '미분류'
            return vendors.get(str(name).strip(), '미분류')
        self.df['_vendor_type'] = self.df['업체명'].apply(get_vendor_type)
        # [FIX-1] vendor_type이 is_domestic 분기에 영향을 주므로 파생값 전체 재계산
        self.df = self._enrich(
            self.df.drop(columns=[c for c in self.df.columns if c.startswith('_')], errors='ignore')
        )
        self._invalidate_cache()

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
        # 자재 STOCK 항목은 실제 날짜 대신 'STOCK' 문자열을 그대로 표시
        if row.get(MATERIAL_ACTUAL_STOCK_COL, False):
            d[MATERIAL_ACTUAL_COL] = 'STOCK'
        if row.get(MATERIAL_PLANNED_STOCK_COL, False):
            d[MATERIAL_PLANNED_COL] = 'STOCK'
        return d

    def get_filtered_df(self, search="", status_filter="", company_filter="", step_filter="", product_filter="", vendor_filter="") -> pd.DataFrame:
        # KPI 집계(get_kpi 등)는 호출 시점 기준으로 _status를 재계산(_refresh_dynamic)하는데
        # 이 함수는 self.df에 캐시된 _status를 그대로 썼었음 — 그 결과 메인 KPI 건수와
        # 팝업(목록) 건수가 날짜가 바뀌면서 달라지는 문제가 있었음(특히 'At Risk').
        # 동일한 기준으로 맞추기 위해 여기서도 재계산을 적용한다.
        df = self._get_refreshed_df()

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
            if status_filter == "지연(전체)":
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
            current_step = row.get('_current_step')
            status = infer_status(row)
            row = row.copy()
            row['_status'] = status
            delay = calc_delay_days(row, current_step=current_step)
            diff = calc_stage_diff(row, current_step=current_step)
            display = get_display_dates(row, status, current_step=current_step)
            return pd.Series({
                '_status': status,
                '_delay_days': delay,
                '_cur_diff': diff.get('cur_diff'),
                '_cur_has_actual': diff.get('cur_has_actual', False),
                '_next_diff': diff.get('next_diff'),
                '_cur_actual_date': display['prev_actual_date'],
                '_current_planned_date': display['current_planned_date'],
            })
        refreshed = df.apply(recompute, axis=1)
        for col in refreshed.columns:
            df[col] = refreshed[col]
        return df

    def _get_fresh_df(self, product_filter: str = "", date_col: str = "", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> pd.DataFrame:
        """필터 적용 + 날짜 재계산된 df 반환. 재계산(_refresh_dynamic) 자체는 필터와 무관하게
        결과가 같으므로, 캐시된 전체 재계산 결과를 먼저 가져온 뒤 필터를 적용한다
        (필터 조합마다 따로 재계산하지 않도록 하기 위함)."""
        df = self._get_refreshed_df()
        if product_filter and product_filter != "전체" and '시스템명' in df.columns:
            pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
            if pf_list:
                df = df[df['시스템명'].isin(pf_list)]
        if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
            df = df[df['_vendor_type'] == vendor_filter]
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
        for step in get_effective_steps(row):
            mapping = STEP_DATE_MAP.get(step, {})
            planned_col = mapping.get('planned')
            actual_col = mapping.get('actual')
            planned = safe_date(row.get(planned_col)) if planned_col else None
            actual = safe_date(row.get(actual_col)) if actual_col else None
            is_done = actual is not None
            if step == '자재' and is_material_stock(row):
                if row.get(MATERIAL_PLANNED_STOCK_COL, False):
                    planned = 'STOCK'
                if row.get(MATERIAL_ACTUAL_STOCK_COL, False):
                    actual = 'STOCK'
                is_done = True
            timeline.append({"step": step, "planned": planned, "actual": actual, "is_current": step == row['_current_step'], "is_done": is_done})

        d['_timeline'] = timeline
        return d

    # ── [BOM개편] 공정 목록 — 수주번호 → 차수(요구납기일) → 아이템 계층 구조 ──────

    def get_grouped_processes(self, page=1, page_size=20, search="", status_filter="",
                               company_filter="", step_filter="", product_filter="",
                               vendor_filter="", sort_by="수주번호", sort_dir="asc") -> Dict:
        """공정 목록 탭용. 최상위=수주번호(지연 섞이면 지연뱃지),
        펼치면 차수(요구납기일 동일값 묶음, 1차/2차.../미정)별 게이지+대표단계,
        차수를 펼치면 그 안의 BOM 아이템 개별 상태까지 내려간다.
        status_filter/step_filter는 '해당 조건을 만족하는 아이템이 하나라도 있는 수주'를
        골라내는 용도로만 쓰고, 골라진 수주 안의 아이템은 전부(필터링 없이) 보여준다
        — 안 그러면 같은 차수 안에서 일부 아이템만 사라져 보여서 맥락을 잃기 때문.

        필터가 하나도 안 걸린 '전체' 조회는 매번 266개 수주・수천개 아이템을 처음부터
        다시 그룹핑하면 느려서, 이 경우에 한해 그룹핑 결과 자체를 캐싱한다(CACHE_TTL_SECONDS).
        검색/필터가 하나라도 걸리면 캐시를 안 쓰고 그때그때 새로 계산한다."""
        no_filters = (
            not search
            and (not status_filter or status_filter == "전체")
            and (not company_filter or company_filter == "전체")
            and (not step_filter or step_filter == "전체")
            and (not product_filter or product_filter == "전체")
            and (not vendor_filter or vendor_filter == "전체")
        )
        if no_filters and self._grouped_cache is not None and (time.time() - self._grouped_cache_at) < self.CACHE_TTL_SECONDS:
            order_groups = self._grouped_cache
        else:
            df = self._get_refreshed_df()
            if df.empty:
                return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 1}

            if vendor_filter and vendor_filter != "전체" and '_vendor_type' in df.columns:
                df = df[df['_vendor_type'] == vendor_filter]
            if company_filter and company_filter != "전체":
                df = df[df['업체명'] == company_filter]
            if product_filter and product_filter != "전체" and '시스템명' in df.columns:
                pf_list = [p.strip() for p in product_filter.split(',') if p.strip()]
                if pf_list:
                    df = df[df['시스템명'].isin(pf_list)]
            if search:
                mask = (
                    df['수주번호'].astype(str).str.contains(search, case=False, na=False) |
                    df['업체명'].astype(str).str.contains(search, case=False, na=False)
                )
                if '프로젝트' in df.columns:
                    mask = mask | df['프로젝트'].astype(str).str.contains(search, case=False, na=False)
                if '시스템명' in df.columns:
                    mask = mask | df['시스템명'].astype(str).str.contains(search, case=False, na=False)
                if '품명' in df.columns:
                    mask = mask | df['품명'].astype(str).str.contains(search, case=False, na=False)
                df = df[mask]

            if df.empty:
                return {"items": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 1}

            order_groups = []
            for order_no, order_df in df.groupby('수주번호', sort=False):
                lots = self._build_lots(order_df)
                is_delayed = any(lot['is_delayed'] for lot in lots)
                rep = order_df.iloc[0]

                # 수주 대표행 보완 — 차수가 여러개라 값 하나로 못 박는 요구납기일 빼고,
                # 병목단계/진척률/이전공정실적일/현재단계예정일은 아이템 전체를 통틀어 집계
                bottleneck_step = None
                bottleneck_idx = None
                if '_current_step' in order_df.columns:
                    for step in order_df['_current_step']:
                        if step not in PROCESS_STEPS:
                            continue
                        idx = PROCESS_STEPS.index(step)
                        if bottleneck_idx is None or idx < bottleneck_idx:
                            bottleneck_idx = idx
                            bottleneck_step = step
                progresses = order_df['_progress'].tolist() if '_progress' in order_df.columns else []
                overall_progress = min(progresses) if progresses else 0

                # 가장 급한(가장 이른) 요구납기일 — 미정(NaT)은 제외, 이미 출고완료된(is_done) 차수도 제외.
                # 즉 "아직 안 나간 차수" 중에서 제일 급한 납기만 본다.
                nearest_due = None
                pending_dues = [lot['요구납기일'] for lot in lots if lot['요구납기일'] and not lot['is_done']]
                if pending_dues:
                    nearest_due = min(pending_dues)

                # 병목 아이템(가장 뒤처진 그 아이템)의 이전공정실적일/현재단계예정일을 대표값으로 사용
                bottleneck_actual_date = None
                bottleneck_planned_date = None
                bottleneck_delayed = False
                if bottleneck_step is not None and '_current_step' in order_df.columns:
                    bn_rows = order_df[order_df['_current_step'] == bottleneck_step]
                    if not bn_rows.empty:
                        bn_row = bn_rows.iloc[0]
                        bottleneck_actual_date = bn_row.get('_cur_actual_date')
                        bottleneck_planned_date = bn_row.get('_current_planned_date')
                        bottleneck_delayed = bool(bn_rows['_status'].isin(DELAY_STATUSES).any())

                # [BOM개편] 마일스톤 바 — 파란구간(공통 완료=overall_progress)까지, 병목이
                # 지연이면 그 단계 폭만큼만 빨간구간을 이어붙인다(병목 단계가 끝나는 지점까지).
                bar_red_end = overall_progress
                if bottleneck_delayed and bottleneck_step in STEP_CUM_END:
                    bar_red_end = min(100, STEP_CUM_END[bottleneck_step])

                # 가장 앞서있는(제일 많이 진행된) 차수의 진척률 — 틱 사이 회색 밴드 끝점으로 사용
                lot_progresses = [lot['progress'] for lot in lots] if lots else [overall_progress]
                bar_ahead_end = max(lot_progresses)

                # 마일스톤 점(출고 지점 고정) — 전체 아이템이 실제 출고(이상) 단계까지 갔는지
                shipped_all = bool(order_df['_status'].isin(SHIPPED_STATUSES).all()) if len(order_df) else False

                order_groups.append({
                    "수주번호": order_no,
                    "업체명": rep.get('업체명'),
                    "프로젝트": rep.get('프로젝트') if '프로젝트' in order_df.columns else None,
                    "_vendor_type": rep.get('_vendor_type'),
                    "item_count": len(order_df),
                    "lot_count": len(lots),
                    "delayed_lot_count": sum(1 for lot in lots if lot['is_delayed']),
                    "is_delayed": is_delayed,
                    "bottleneck_step": bottleneck_step,
                    "progress": overall_progress,
                    "nearest_due_date": nearest_due,
                    "bottleneck_actual_date": bottleneck_actual_date,
                    "bottleneck_planned_date": bottleneck_planned_date,
                    "bar_blue_end": overall_progress,
                    "bar_red_end": bar_red_end,
                    "bar_ahead_end": bar_ahead_end,
                    "ship_milestone_pct": SHIP_MILESTONE_PCT,
                    "shipped_all": shipped_all,
                    "lots": lots,
                })

            if no_filters:
                self._grouped_cache = order_groups
                self._grouped_cache_at = time.time()

        # status_filter/step_filter: 조건을 만족하는 아이템이 하나라도 있는 수주만 남긴다
        if status_filter and status_filter != "전체":
            if status_filter == "지연(전체)":
                order_groups = [g for g in order_groups if g["is_delayed"]]
            else:
                order_groups = [
                    g for g in order_groups
                    if any(item.get('_status') == status_filter for lot in g["lots"] for item in lot["items"])
                ]
        if step_filter and step_filter != "전체":
            order_groups = [
                g for g in order_groups
                if any(item.get('_current_step') == step_filter for lot in g["lots"] for item in lot["items"])
            ]

        # 캐시 히트 시 self._grouped_cache와 동일한 리스트 객체이므로, 정렬은 반드시
        # 새 리스트에 대해 해야 한다(그대로 .sort()하면 캐시 원본 순서가 오염됨).
        order_groups = list(order_groups)
        reverse = (sort_dir != "asc")
        if sort_by == "업체명":
            order_groups.sort(key=lambda g: str(g.get("업체명") or ""), reverse=reverse)
        else:
            order_groups.sort(key=lambda g: str(g["수주번호"]), reverse=reverse)

        total = len(order_groups)
        total_pages = max(1, math.ceil(total / page_size))
        page = max(1, min(page, total_pages))
        start = (page - 1) * page_size
        page_items = order_groups[start:start + page_size]

        return {"items": page_items, "total": total, "page": page, "page_size": page_size, "total_pages": total_pages}

    def _build_lots(self, order_df: pd.DataFrame) -> List[Dict]:
        """수주번호 하나 안에서 요구납기일 값으로 차수를 나눈다.
        정확히 같은 날짜만 같은 차수. 요구납기일 없으면 '미정' 차수로 별도 그룹.
        차수번호는 요구납기일 오름차순으로 1차가 가장 빠른 납기."""
        if '요구납기일' in order_df.columns:
            has_due = order_df['요구납기일'].notna()
        else:
            has_due = pd.Series(False, index=order_df.index)

        dated_df = order_df[has_due]
        undated_df = order_df[~has_due]

        lot_groups = []
        if not dated_df.empty:
            for due_date, sub_df in dated_df.groupby('요구납기일', sort=True):
                lot_groups.append((pd.Timestamp(due_date), sub_df))
            lot_groups.sort(key=lambda x: x[0])
        if not undated_df.empty:
            lot_groups.append((None, undated_df))  # 미정차수는 항상 맨 뒤

        lots = []
        lot_no = 0
        for due_date, sub_df in lot_groups:
            if due_date is not None:
                lot_no += 1
                label = f"{lot_no}차"
            else:
                label = "미정"
            lots.append(self._summarize_lot(label, due_date, sub_df))
        return lots

    def _summarize_lot(self, label: str, due_date, sub_df: pd.DataFrame) -> Dict:
        items = [self._row_to_dict(row) for _, row in sub_df.iterrows()]
        statuses = sub_df['_status'].tolist() if '_status' in sub_df.columns else []
        is_delayed = any(s in DELAY_STATUSES for s in statuses)

        # 대표단계(병목) = PROCESS_STEPS 순서상 가장 뒤처진(인덱스가 가장 낮은) 아이템의 현재단계
        # (스킵규칙은 이미 아이템별 infer_current_step에서 반영되어 있음)
        bottleneck_step = None
        bottleneck_idx = None
        if '_current_step' in sub_df.columns:
            for step in sub_df['_current_step']:
                if step not in PROCESS_STEPS:
                    continue
                idx = PROCESS_STEPS.index(step)
                if bottleneck_idx is None or idx < bottleneck_idx:
                    bottleneck_idx = idx
                    bottleneck_step = step

        progresses = sub_df['_progress'].tolist() if '_progress' in sub_df.columns else []
        lot_progress = min(progresses) if progresses else 0

        DONE_LIKE = {'출고완료', '계산서완료', '출고지연', 'OTP지연', '계산서지연'}
        is_all_done = bool(statuses) and all(s in DONE_LIKE for s in statuses)

        return {
            "label": label,
            "요구납기일": safe_date(due_date) if due_date is not None else None,
            "item_count": len(sub_df),
            "delayed_item_count": sum(1 for s in statuses if s in DELAY_STATUSES),
            "is_delayed": is_delayed,
            "bottleneck_step": bottleneck_step,
            "progress": lot_progress,
            "is_done": is_all_done,
            "items": items,
        }

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
            status = infer_status(row)
            display = get_display_dates(row, status)  # [FIX-2]
            self.df.at[idx, '_status'] = status
            self.df.at[idx, '_progress'] = calc_progress(row)
            self.df.at[idx, '_delay_days'] = calc_delay_days(row)
            self.df.at[idx, '_cur_diff'] = diff.get('cur_diff')
            self.df.at[idx, '_cur_has_actual'] = diff.get('cur_has_actual', False)
            self.df.at[idx, '_next_diff'] = diff.get('next_diff')
            self.df.at[idx, '_cur_actual_date'] = display['prev_actual_date']      # [FIX-2]
            self.df.at[idx, '_current_planned_date'] = display['current_planned_date']   # [FIX-2]

        try:
            save_df = self.df.drop(columns=[c for c in self.df.columns if c.startswith('_')], errors='ignore')
            save_df.to_excel(self.filepath, index=False)
        except Exception as e:
            print(f"[DataManager] Save error: {e}")
            return False

        self._invalidate_cache()
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

        completed = len(df[df['_status'].isin(['출고완료', '계산서완료', '출고지연', 'OTP지연', '계산서지연'])])
        delivered = len(df[df['_status'].isin(['출고완료', '출고지연'])])
        invoiced  = len(df[df['_status'].isin(['계산서완료', 'OTP지연', '계산서지연'])])
        data_error = len(df[df['_status'] == '데이터오류'])
        avg_progress = int(df['_progress'].mean()) if total > 0 else 0

        system_counts = {}
        system_completed = {}
        system_delayed = {}
        DELAY_STATUSES_ALL = ['지연', '출고지연', 'OTP지연', '계산서지연']
        if '시스템명' in df.columns:
            for sys, grp in df.groupby('시스템명'):
                system_counts[str(sys)] = len(grp)
                system_completed[str(sys)] = len(grp[grp['_status'].isin(['출고완료', '계산서완료', '출고지연', 'OTP지연', '계산서지연'])])
                system_delayed[str(sys)] = len(grp[grp['_status'].isin(DELAY_STATUSES_ALL)])

        in_progress = on_track + at_risk + delayed_process

        # 이달 출고예정: 요구납기일이 이번 달이고 아직 출고(최종납기일) 안 된 건 (alerts의 due_soon_출고와 동일 기준)
        due_this_month = 0
        if '요구납기일' in df.columns:
            today = pd.Timestamp.now()
            this_month_start = today.replace(day=1)
            next_month_start = this_month_start + pd.DateOffset(months=1)
            mask = (df['요구납기일'].notna() &
                    (df['요구납기일'].dt.date >= this_month_start.date()) &
                    (df['요구납기일'].dt.date < next_month_start.date()) &
                    df['최종납기일'].isna())
            due_this_month = int(mask.sum())

        return {"total": total, "in_progress": in_progress, "on_track": on_track,
                "at_risk": at_risk,
                "delayed": delayed_total,
                "delayed_process": delayed_process,
                "delayed_delivery": delayed_delivery,
                "delayed_post": delayed_post,
                "due_this_month": due_this_month,
                "completed": completed, "delivered": delivered, "invoiced": invoiced,
                "data_error": data_error,
                "avg_progress": avg_progress,
                "system_counts": system_counts, "system_completed": system_completed, "system_delayed": system_delayed}

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
            return {"delayed": [], "at_risk": [], "month_upcoming": [], "due_soon": {"출고": [], "OTP": []}, "data_error": []}

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
                "시스템명": row.get('시스템명', ''),
                "_current_step": row.get('_current_step', ''),
                "_progress": int(row.get('_progress', 0)),
                "_vendor_type": row.get('_vendor_type', '미분류'),
                "_status": row.get('_status', ''),
                "_delay_days": int(row['_delay_days']) if pd.notna(row.get('_delay_days')) else 0,
                "요구납기일": safe_date(row.get('요구납기일')),
                "OTP예상일": safe_date(row.get('OTP예상일')),
            }

        # 요구납기일 초과: 지연 계열 상태이면서 실제로 요구납기일이 이미 지난 건만
        DELAY_STATUSES_ALL = ['지연', '출고지연', 'OTP지연', '계산서지연']
        overdue_mask = df['_status'].isin(DELAY_STATUSES_ALL) & df['요구납기일'].notna() & (df['요구납기일'] < today)
        delayed_df = df[overdue_mask].copy()
        # 그중에서도 출고 자체가 안 나간 건(출고지연)을 맨 위로, 그 안에서는 지연일수 내림차순
        delayed_df['_prio'] = (delayed_df['_status'] != '출고지연').astype(int)
        delayed_df = delayed_df.sort_values(['_prio', '_delay_days'], ascending=[True, False])
        delayed = [row_summary(row) for _, row in delayed_df.iterrows()]
        at_risk = [row_summary(row) for _, row in df[df['_status'] == 'At Risk'].iterrows()]

        # 다가오는 일정: 요구납기일 초과에 안 들어간 건(완료/데이터오류 제외) 전부 —
        # 정상/임박은 이번 달 예정인 것만, 아직 요구납기일이 안 지난 지연류는 달과 무관하게 항상 표시
        # (그래야 KPI 지연 합계가 요구납기일초과+다가오는일정 어딘가엔 반드시 다 잡힘)
        month_upcoming = []
        not_overdue = df[~overdue_mask & ~df['_status'].isin(['출고완료', '계산서완료', '데이터오류'])]
        for _, row in not_overdue.iterrows():
            status = row.get('_status', '')
            is_delay_type = status in DELAY_STATUSES_ALL
            planned = get_display_dates(row).get('current_planned_date')
            if not planned and is_delay_type:
                planned = safe_date(row.get('요구납기일'))  # 예정일 컬럼 자체가 없는 지연류는 요구납기일로라도 표시
            if not planned:
                continue
            p_ts = pd.Timestamp(planned)
            in_this_month = this_month_start.date() <= p_ts.date() < next_month_start.date()
            if not in_this_month and not is_delay_type:
                continue
            item = row_summary(row)
            item['_next_planned_date'] = planned
            item['_dday'] = int((p_ts.normalize() - today.normalize()).days)
            month_upcoming.append(item)
        month_upcoming.sort(key=lambda x: x['_next_planned_date'])

        due_soon_출고 = []
        if '요구납기일' in df.columns:
            # 월별 출고예정 차트와 동일 기준: 요구납기일이 이번 달 + 아직 최종납기일(납품) 없는 건 (이미 지연된 건도 포함)
            mask = (df['요구납기일'].notna() &
                    (df['요구납기일'].dt.date >= this_month_start.date()) &
                    (df['요구납기일'].dt.date < next_month_start.date()) &
                    df['최종납기일'].isna())
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

        return {"delayed": delayed, "at_risk": at_risk, "month_upcoming": month_upcoming,
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
        """요구납기일 초과 TOP5: 요구납기일 기준 지연일수로 정렬"""
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)  # [FIX-3]
        today = pd.Timestamp.now()
        delayed = df[df['_status'].isin(['지연', '출고지연'])].copy()

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

        step_order = {s: i for i, s in enumerate(PROCESS_STEPS)}
        cur_step_idx = df['_current_step'].map(step_order)

        result = []
        for step in PROCESS_STEPS:
            if step == '수주':
                continue
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
            # 지연 건수: KPI/상태분포와 동일하게 _status 기준으로 통일 (기존 cur_diff>0 기준에서 변경)
            DELAY_STATUSES = {'지연', '출고지연', 'OTP지연', '계산서지연'}
            delayed_rows = [r for _, r in step_df.iterrows() if r.get('_status') in DELAY_STATUSES]
            cur_diffs = [r['_cur_diff'] for r in delayed_rows
                         if r.get('_cur_diff') is not None and not (isinstance(r['_cur_diff'], float) and pd.isna(r['_cur_diff']))]
            avg_cur = round(sum(cur_diffs) / len(cur_diffs)) if cur_diffs else None
            delayed_count = len(delayed_rows)

            # 모드2: 다음 일정 초과 평균 — 전체 건 기준, 미초과=0 포함
            next_diffs = [max(0, r['_next_diff']) for _, r in step_df.iterrows()
                          if r.get('_next_diff') is not None and not (isinstance(r['_next_diff'], float) and pd.isna(r['_next_diff']))]
            avg_next = round(sum(next_diffs) / len(next_diffs)) if next_diffs else None

            # 완료 건수: 실적일 컬럼의 존재 여부가 아니라, 이미 계산된 현재단계(_current_step) 위치 기준으로 판단.
            # (실적일만 보면 중간 단계 데이터 누락 시 순서가 깨짐 — infer_current_step이 이미 그 누락/스킵을
            #  반영해서 현재단계를 정했으므로, 그 위치보다 뒤에 있으면 이 단계는 완료로 본다)
            idx = step_order[step]
            completed_count = int((cur_step_idx > idx).sum())
            waiting_count = int((cur_step_idx < idx).sum())

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
                "completed_count": completed_count,
                "waiting_count": waiting_count,
            })
        return result

    def get_stage_delayed_items(self, step: str, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> List[Dict]:
        """단계별 평균 지연일수 차트 클릭 시 — 해당 단계의 지연 건 목록 (차트와 동일 기준)"""
        if self.df.empty:
            return []
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)
        step_df = df[df['_current_step'] == step]

        DELAY_STATUSES = {'지연', '출고지연', 'OTP지연', '계산서지연'}
        result = []
        for _, row in step_df.iterrows():
            status = row.get('_status', '')
            if status not in DELAY_STATUSES:
                continue
            cur_diff = row.get('_cur_diff')
            if cur_diff is None or (isinstance(cur_diff, float) and pd.isna(cur_diff)):
                cur_diff = 0

            result.append({
                "수주번호":         row.get('수주번호', ''),
                "프로젝트":         row.get('프로젝트', ''),
                "업체명":           row.get('업체명', ''),
                "시스템명":         row.get('시스템명', ''),
                "_current_step":    row.get('_current_step', ''),
                "_status":          status,
                "_cur_diff":        int(cur_diff),
                "_cur_actual_date": row.get('_cur_actual_date'),
                "_current_planned_date": row.get('_current_planned_date'),
                "_progress":        row.get('_progress', 0),
                "_row_id":          row.get('_row_id', ''),
                "ordseq":           row.get('ordseq'),
            })

        result.sort(key=lambda x: x['_cur_diff'], reverse=True)
        return result

    def get_all_delayed_items(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> List[Dict]:
        """전체 지연 건 목록 (지연 관리 탭용) — 공정중지연/출고지연/OTP지연/계산서지연 모두 포함"""
        if self.df.empty:
            return []
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)

        DELAY_STATUSES = {'지연', '출고지연', 'OTP지연', '계산서지연'}
        result = []
        for _, row in df.iterrows():
            status = row.get('_status', '')
            if status not in DELAY_STATUSES:
                continue
            cur_diff = row.get('_cur_diff')
            if cur_diff is None or (isinstance(cur_diff, float) and pd.isna(cur_diff)):
                cur_diff = 0

            result.append({
                "수주번호":         row.get('수주번호', ''),
                "프로젝트":         row.get('프로젝트', ''),
                "업체명":           row.get('업체명', ''),
                "시스템명":         row.get('시스템명', ''),
                "_current_step":    row.get('_current_step', ''),
                "_status":          status,
                "_cur_diff":        int(cur_diff),
                "_cur_actual_date": row.get('_cur_actual_date'),
                "_current_planned_date": row.get('_current_planned_date'),
                "_progress":        row.get('_progress', 0),
                "요구납기일":       safe_date(row.get('요구납기일')),
                "_row_id":          row.get('_row_id', ''),
                "ordseq":           row.get('ordseq'),
            })

        result.sort(key=lambda x: x['_cur_diff'], reverse=True)
        return result

    def get_status_distribution(self, product_filter: str = "", date_col: str = "요구납기일", date_from: str = "", date_to: str = "", vendor_filter: str = "") -> Dict:
        """전체 상태 분포 (도넛차트용) — 정상/임박/지연/출고/OTP/계산서 6분류, 총합이 항상 total과 일치하도록
        우선순위(계산서 > OTP > 출고 > 진행중상태) 기준으로 완전히 배타적으로 분류"""
        if self.df.empty:
            return {}
        df = self._get_fresh_df(product_filter, date_col, date_from, date_to, vendor_filter)
        total = len(df)

        invoiced_mask = df['계산서발행일'].notna() if '계산서발행일' in df.columns else pd.Series(False, index=df.index)
        otp_mask = (~invoiced_mask) & (df['OTP일자'].notna() if 'OTP일자' in df.columns else pd.Series(False, index=df.index))
        shipped_mask = (~invoiced_mask) & (~otp_mask) & (df['최종납기일'].notna() if '최종납기일' in df.columns else pd.Series(False, index=df.index))
        pending_mask = ~invoiced_mask & ~otp_mask & ~shipped_mask

        pending_df = df[pending_mask]
        DELAY_STATUSES_ALL = ['지연', '출고지연', 'OTP지연', '계산서지연']
        return {
            "total": total,
            "on_track": int(len(pending_df[pending_df['_status'] == 'On Track'])),
            "at_risk":  int(len(pending_df[pending_df['_status'] == 'At Risk'])),
            "delayed":  int(len(pending_df[pending_df['_status'].isin(DELAY_STATUSES_ALL)])),
            "shipped":  int(shipped_mask.sum()),   # 출고: 최종납기일 있음, OTP/계산서 전
            "otp":      int(otp_mask.sum()),       # OTP: OTP일자 있음, 계산서 전
            "invoiced": int(invoiced_mask.sum()),  # 계산서: 계산서발행일 있음
            "data_error": int(len(df[df['_status'] == '데이터오류'])),
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
                "_vendor_type": row.get('_vendor_type', ''),
                "요구납기일": safe_date(row.get('요구납기일')),
                "최종납기일": safe_date(row.get('최종납기일')),
                "OTP일자": safe_date(row.get('OTP일자')),
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
