import streamlit as st
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import plotly.graph_objects as go
import io
import os
import time
import json
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# 1. 디자인 시스템 & 설정
# ==========================================
DAEDONG = {
    'red': '#EF4023', 'black': '#231F20', 'light_gray': '#DCDCD7',
    'medium_gray': '#9E9F9C', 'dark_gray': '#585958', 'future_green_light': '#6AC8C7',
    'future_green': '#00677B', 'white': '#FFFFFF', 'blue': '#4DA8DA'
}

LINE_COLORS = {
    '내부온도(xintemp1)': '#6AC8C7',
    '내부습도(xinhum1)': '#EF4023',
    'CO2농도(xco2)':     '#DCDCD7',
}
THRESHOLD_COLORS = {
    '내부온도(xintemp1)': {'upper': '#FFD700', 'lower': '#FF8C00'},
    '내부습도(xinhum1)': {'upper': '#FF69B4', 'lower': '#DA70D6'},
    'CO2농도(xco2)':     {'upper': '#00FF7F', 'lower': '#00CED1'},
}

ZONE_CONFIG = {
    1: {'name': '1구역', 'greenhouse': '2ha 온실'},
    2: {'name': '2구역', 'greenhouse': '2ha 온실'},
    3: {'name': '3구역', 'greenhouse': '2ha 온실'},
    4: {'name': '4구역', 'greenhouse': '2ha 온실'},
    5: {'name': '5구역', 'greenhouse': '1ha 온실'},
    6: {'name': '6구역', 'greenhouse': '1ha 온실'},
}

MONTH_NAMES = {
    1:'1월', 2:'2월', 3:'3월', 4:'4월', 5:'5월', 6:'6월',
    7:'7월', 8:'8월', 9:'9월', 10:'10월', 11:'11월', 12:'12월'
}

st.set_page_config(page_title="그린스케이프 스마트팜 경영 진단 리포트", layout="wide", initial_sidebar_state="expanded")

st.markdown(f"""
    <style>
    .stApp {{ background-color: {DAEDONG['black']}; color: {DAEDONG['light_gray']}; }}
    [data-testid="stSidebar"] {{ background-color: {DAEDONG['dark_gray']}; }}
    h1, h2, h3, h4 {{ color: {DAEDONG['white']}; }}
    .report-title {{ font-size: 28px; font-weight: bold; color: {DAEDONG['white']}; margin-bottom: 0px; }}
    .report-subtitle {{ font-size: 14px; color: {DAEDONG['medium_gray']}; }}
    hr {{ border-color: {DAEDONG['medium_gray']}; }}
    @media print {{
        body, .stApp, .main {{ background-color: {DAEDONG['black']} !important; -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }}
        [data-testid="stSidebar"], [data-testid="stHeader"], [data-testid="stToolbar"], button {{ display: none !important; }}
        .stPlotlyChart, .stDataFrame {{ page-break-inside: avoid; }}
    }}
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. 월별 우수농가 가이드라인 데이터
# ==========================================
GUIDE_DATA = {
    1:  {'avg_t': 16.8, 'night_t': 19.3, 'day_t': 14.6, 'diff_t': 4.7, 'sun': 872,  'hum': 84.4, 'co2': 400},
    2:  {'avg_t': 17.4, 'night_t': 20.6, 'day_t': 15.0, 'diff_t': 5.6, 'sun': 1167, 'hum': 82.2, 'co2': 373},
    3:  {'avg_t': 18.5, 'night_t': 21.3, 'day_t': 15.6, 'diff_t': 5.7, 'sun': 1419, 'hum': 76.1, 'co2': 355},
    4:  {'avg_t': 19.5, 'night_t': 22.4, 'day_t': 16.1, 'diff_t': 6.2, 'sun': 1763, 'hum': 70.6, 'co2': 345},
    5:  {'avg_t': 21.4, 'night_t': 23.8, 'day_t': 17.3, 'diff_t': 6.5, 'sun': 1916, 'hum': 70.3, 'co2': 336},
    6:  {'avg_t': 24.4, 'night_t': 26.4, 'day_t': 20.2, 'diff_t': 6.2, 'sun': 1888, 'hum': 71.2, 'co2': 341},
    7:  {'avg_t': 26.8, 'night_t': 28.9, 'day_t': 23.7, 'diff_t': 5.2, 'sun': 1589, 'hum': 75.5, 'co2': 368},
    8:  {'avg_t': 27.0, 'night_t': 30.2, 'day_t': 24.4, 'diff_t': 5.7, 'sun': 1656, 'hum': 65.5, 'co2': 334},
    9:  {'avg_t': 23.4, 'night_t': 26.3, 'day_t': 20.4, 'diff_t': 6.0, 'sun': 1334, 'hum': 67.9, 'co2': 329},
    10: {'avg_t': 19.7, 'night_t': 23.2, 'day_t': 17.4, 'diff_t': 5.8, 'sun': 1249, 'hum': 71.7, 'co2': 339},
    11: {'avg_t': 17.9, 'night_t': 20.8, 'day_t': 15.6, 'diff_t': 5.1, 'sun': 938,  'hum': 79.7, 'co2': 380},
    12: {'avg_t': 16.9, 'night_t': 19.1, 'day_t': 14.6, 'diff_t': 4.5, 'sun': 793,  'hum': 83.2, 'co2': 404},
}

def get_default_thresholds(guide, pct=0.05):
    """가이드값 기준 ±5% 로 min/max 계산"""
    return {
        '내부온도(xintemp1)': (
            round(guide['avg_t'] * (1 - pct), 1),
            round(guide['avg_t'] * (1 + pct), 1)
        ),
        '내부습도(xinhum1)': (
            round(max(0,   guide['hum'] * (1 - pct)), 1),
            round(min(100, guide['hum'] * (1 + pct)), 1)
        ),
        'CO2농도(xco2)': (
            round(max(0, guide['co2'] * (1 - pct)), 1),
            round(guide['co2'] * (1 + pct), 1)
        ),
    }

# ==========================================
# 3. Google Sheets 연동 로직
# ==========================================
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
HEADERS = ["xdatetime", "내부온도(xintemp1)", "내부습도(xinhum1)", "CO2농도(xco2)", "누적일사량(xsunadd)", "주야간(xjuya)"]

try:
    API_BASE = st.secrets["API_BASE"]
except Exception:
    API_BASE = "http://api.gcsmagma.com/gcs_my_api.php/Get_GCS_Data/tasmart"

try:
    SHEET_ID = st.secrets["SHEET_ID"]
except Exception:
    SHEET_ID = ""

@st.cache_resource
def get_gsheet_client():
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], scopes=SCOPES
        )
        return gspread.authorize(creds)
    except Exception:
        return None

def get_or_create_worksheet(client, zone_id):
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet_name = f"zone{zone_id}"
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=50000, cols=10)
            worksheet.append_row(HEADERS)
        return worksheet
    except Exception:
        return None

def safe_float(val):
    try:
        if val == "" or pd.isna(val) or val is None:
            return ""
        f = float(val)
        return "" if np.isnan(f) else f
    except:
        return ""

def to_sheet_val(v):
    if v is None or v == "":
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""
    return v

def fetch_and_save_data(zone_id):
    url = f"{API_BASE}/{zone_id}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code != 200:
            return False, f"[구역{zone_id}] HTTP 오류: {resp.status_code}"
        try:
            data = resp.json()
        except json.JSONDecodeError:
            return False, f"[구역{zone_id}] JSON 파싱 실패"
        if "fields" not in data or len(data["fields"]) == 0:
            return False, f"[구역{zone_id}] fields 안에 데이터 없음"
        row = data["fields"][0]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_row = [
            current_time,
            to_sheet_val(safe_float(row.get("xintemp1"))),
            to_sheet_val(safe_float(row.get("xinhum1"))),
            to_sheet_val(safe_float(row.get("xco2"))),
            to_sheet_val(safe_float(row.get("xsunadd"))),
            to_sheet_val(safe_float(row.get("xjuya"))),
        ]
        client = get_gsheet_client()
        if not client:
            return False, f"[구역{zone_id}] Google 인증 실패"
        worksheet = get_or_create_worksheet(client, zone_id)
        if not worksheet:
            return False, f"[구역{zone_id}] 시트 접근 실패"
        worksheet.append_row(new_row, value_input_option="USER_ENTERED")
        return True, f"[구역{zone_id}] Sheets 저장 성공 ({current_time})"
    except requests.exceptions.Timeout:
        return False, f"[구역{zone_id}] API 연결 시간 초과"
    except Exception as e:
        return False, f"[구역{zone_id}] 오류: {e}"

@st.cache_data(ttl=55)
def load_data(zone_id):
    client = get_gsheet_client()
    if not client:
        return pd.DataFrame(), "Google 인증 실패"
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        try:
            worksheet = spreadsheet.worksheet(f"zone{zone_id}")
        except gspread.exceptions.WorksheetNotFound:
            return pd.DataFrame(), "저장된 데이터가 없습니다."
        all_values = worksheet.get_all_values()
        if len(all_values) < 2:
            return pd.DataFrame(), "저장된 데이터가 없습니다."
        first_row = all_values[0]
        if first_row[0] == 'xdatetime':
            df = pd.DataFrame(all_values[1:], columns=all_values[0])
        else:
            df = pd.DataFrame(all_values, columns=HEADERS)
        df = df[df['xdatetime'] != '']
        if df.empty:
            return pd.DataFrame(), "저장된 데이터가 없습니다."
        df['xdatetime'] = pd.to_datetime(df['xdatetime'], errors='coerce')
        df = df.dropna(subset=['xdatetime'])
        for col in ['내부온도(xintemp1)', '내부습도(xinhum1)', 'CO2농도(xco2)', '누적일사량(xsunadd)', '주야간(xjuya)']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df, "성공"
    except Exception as e:
        return pd.DataFrame(), f"Sheets 읽기 오류: {e}"

# ==========================================
# 4. 사이드바 구성
# ==========================================
if os.path.exists("image_8a62a0.png"):
    st.sidebar.image("image_8a62a0.png", use_container_width=True)
else:
    st.sidebar.markdown(f"<h2 style='color:{DAEDONG[\"red\"]}'>DAEDONG</h2>", unsafe_allow_html=True)

st.sidebar.markdown("---")
st.sidebar.subheader("🏡 온실 및 구역 선택")
greenhouse_options = ["2ha 온실 (1~4구역)", "1ha 온실 (5~6구역)"]
selected_greenhouse = st.sidebar.radio("온실 선택", greenhouse_options, index=0)

if "2ha" in selected_greenhouse:
    zone_options = {1: "1구역", 2: "2구역", 3: "3구역", 4: "4구역"}
else:
    zone_options = {5: "5구역", 6: "6구역"}

selected_zone_label = st.sidebar.selectbox("구역 선택", options=list(zone_options.values()), index=0)
selected_zone_id = [k for k, v in zone_options.items() if v == selected_zone_label][0]

# API 수집 (fetch 최적화: 55초 캐시)
now = time.time()
last_fetch = st.session_state.get("last_fetch_time", 0)
need_fetch = (now - last_fetch) >= 55

if need_fetch:
    all_results = {}
    for zid in ZONE_CONFIG.keys():
        success, msg = fetch_and_save_data(zid)
        all_results[zid] = (success, msg)
    st.session_state["last_fetch_results"] = all_results
    st.session_state["last_fetch_time"] = now
else:
    all_results = st.session_state.get("last_fetch_results", {
        zid: (False, "대기 중") for zid in ZONE_CONFIG.keys()
    })

api_success, api_msg = all_results.get(selected_zone_id, (False, "대기 중"))
df_all, load_msg = load_data(selected_zone_id)

st.sidebar.markdown("---")
st.sidebar.subheader("🔄 데이터 동기화")
auto_refresh = st.sidebar.checkbox("자동 새로고침 켜기 (1분 주기)", value=True)
if st.sidebar.button("지금 수동 새로고침"):
    st.cache_data.clear()
    st.session_state["last_fetch_time"] = 0
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("📊 기간 및 데이터 필터")
period = st.sidebar.radio("조회 주기 선택", ["전체 데이터 (누적)", "최근 1일 (24h)"], index=0)

if period == "최근 1일 (24h)" and len(df_all) > 0:
    last_24h = df_all['xdatetime'].max() - pd.Timedelta(hours=24)
    df_all = df_all[df_all['xdatetime'] >= last_24h]

available_metrics = ['내부온도(xintemp1)', '내부습도(xinhum1)', 'CO2농도(xco2)']
selected_metrics = st.sidebar.multiselect("분석 대상 선택", available_metrics, default=available_metrics[:2])

# ==========================================
# 가이드 설정 사이드바
# ==========================================
st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ 가이드 설정")

current_month = datetime.now().month

# ✅ 월 선택 → 불러오기
month_list = [f"{m}월" for m in range(1, 13)]
selected_guide_month_label = st.sidebar.selectbox(
    "기준월 선택",
    options=month_list,
    index=current_month - 1,
    key="guide_month_select"
)
selected_guide_month = int(selected_guide_month_label.replace("월", ""))

if st.sidebar.button("📥 선택 월 가이드 불러오기", use_container_width=True):
    guide = GUIDE_DATA[selected_guide_month]
    defaults = get_default_thresholds(guide)
    for metric in available_metrics:
        mn, mx = defaults[metric]
        st.session_state[f"guide_min_{metric}"] = mn
        st.session_state[f"guide_max_{metric}"] = mx
    st.session_state["guide_loaded_month"] = selected_guide_month
    st.sidebar.success(f"✅ {selected_guide_month}월 가이드 불러옴 (±5%)")

# 로드된 월 표시
loaded_month = st.session_state.get("guide_loaded_month", None)
if loaded_month:
    st.sidebar.caption(f"현재 적용 중: {loaded_month}월 기준 ±5%")

# ✅ 초기값: session_state 없으면 현재 월 ±5% 기본값
month_guide = GUIDE_DATA.get(current_month, GUIDE_DATA[3])
default_thresholds = get_default_thresholds(month_guide)
for metric in available_metrics:
    if f"guide_min_{metric}" not in st.session_state:
        st.session_state[f"guide_min_{metric}"] = default_thresholds[metric][0]
    if f"guide_max_{metric}" not in st.session_state:
        st.session_state[f"guide_max_{metric}"] = default_thresholds[metric][1]

# ✅ 수정 가능한 min/max 입력 (불러온 후에도 직접 수정 가능)
thresholds = {}
metric_labels = {
    '내부온도(xintemp1)': '🌡 온도',
    '내부습도(xinhum1)': '💧 습도',
    'CO2농도(xco2)':     '🌿 CO2',
}
for metric in selected_metrics:
    st.sidebar.markdown(f"**{metric_labels.get(metric, metric)}**")
    c1, c2 = st.sidebar.columns(2)
    mn = c1.number_input(
        "Min", step=0.5,
        value=float(st.session_state[f"guide_min_{metric}"]),
        key=f"input_min_{metric}"
    )
    mx = c2.number_input(
        "Max", step=0.5,
        value=float(st.session_state[f"guide_max_{metric}"]),
        key=f"input_max_{metric}"
    )
    # session_state 동기화
    st.session_state[f"guide_min_{metric}"] = mn
    st.session_state[f"guide_max_{metric}"] = mx
    thresholds[metric] = (mn, mx)

# ==========================================
# 5. 상단 UI
# ==========================================
zone_info = ZONE_CONFIG[selected_zone_id]

col_logo, col_empty, col_info_area = st.columns([1.5, 0.5, 2.5])
with col_logo:
    if os.path.exists("image_8a62a0.png"):
        st.image("image_8a62a0.png", width=200)
    else:
        st.markdown(f"<h1 style='color:{DAEDONG['red']}'>DAEDONG</h1>", unsafe_allow_html=True)

with col_info_area:
    st.markdown(f"""
        <div style="text-align: right;">
            <div class="report-subtitle">최근 갱신: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            <div class="report-title">그린스케이프 님의 스마트팜 진단 결과입니다</div>
            <div class="report-subtitle">
                태안팜 | {zone_info['greenhouse']} - {zone_info['name']} | 누적 데이터 수: {len(df_all)}건
            </div>
        </div>
    """, unsafe_allow_html=True)

    export_col1, export_col2 = st.columns([1, 1])
    with export_col1:
        if len(df_all) > 0:
            df_display = df_all.copy().sort_values(by='xdatetime', ascending=False)
            df_display['xdatetime'] = pd.to_datetime(df_display['xdatetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_display.to_excel(writer, index=False, sheet_name='Farm_Data')
            st.download_button(
                "📊 데이터 엑셀(.xlsx) 저장",
                data=buffer.getvalue(),
                file_name=f"farm_zone{selected_zone_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.ms-excel",
                use_container_width=True
            )
        else:
            st.button("📊 데이터 없음", disabled=True, use_container_width=True)
    with export_col2:
        st.markdown(f"""<button onclick="window.print()" style="width:100%; background-color:{DAEDONG['future_green']}; color:white; padding:6px 0px; border:none; border-radius:4px; font-size:16px; cursor:pointer;">📄 화면 PDF 인쇄</button>""", unsafe_allow_html=True)

st.markdown("<hr style='margin-top: 5px; margin-bottom: 10px;'>", unsafe_allow_html=True)

if not api_success:
    st.error(f"⚠️ [API/Sheets 실패] {api_msg}")
else:
    st.success(f"✅ [저장 성공] {api_msg}")

if load_msg != "성공" and load_msg != "저장된 데이터가 없습니다.":
    st.error(f"⚠️ [데이터 로드 실패] {load_msg}")

if len(df_all) == 0:
    st.warning("현재 화면에 표시할 데이터가 없습니다. 잠시 후 새로고침 해주세요.")
    st.stop()

# ==========================================
# 6. 상단 요약 카드 + 포커스 모드
# ==========================================
def analyze_violation(data_series, min_th, max_th):
    valid_data = data_series.dropna()
    total = len(valid_data)
    if total == 0: return 0, 0
    high = len(valid_data[valid_data > max_th])
    low  = len(valid_data[valid_data < min_th])
    return round(((total - high - low) / total) * 100, 1), (high + low)

if "focused_metric" not in st.session_state:
    st.session_state["focused_metric"] = None

if len(selected_metrics) > 0:
    valid_metrics = [m for m in selected_metrics if m in df_all.columns]

    if valid_metrics:
        cols = st.columns(len(valid_metrics))
        col_idx = 0

        c_dark   = DAEDONG['dark_gray']
        c_medium = DAEDONG['medium_gray']
        c_light  = DAEDONG['light_gray']
        c_white  = DAEDONG['white']
        c_red    = DAEDONG['red']

        for metric in selected_metrics:
            if metric not in df_all.columns:
                continue

            min_t, max_t = thresholds[metric]
            series       = df_all[metric].dropna()
            all_zero     = len(series) == 0 or (series == 0).all()
            is_focused   = (st.session_state["focused_metric"] == metric)
            tc           = THRESHOLD_COLORS.get(metric, {'upper': '#FFD700', 'lower': '#FF8C00'})
            border_color = tc['upper'] if is_focused else c_medium
            border_width = "2px" if is_focused else "1px"

            if all_zero:
                ratio_html  = f'<div style="color:{c_medium}; font-size:20px; font-weight:bold; margin-bottom:15px;">센서 데이터 없음</div>'
                bottom_html = f'<div style="color:{c_medium}; font-size:15px;">수집된 유효 데이터가 없습니다</div>'
                signal_html = f'<div style="font-size:15px; color:{c_medium}; margin-top:14px;">현재값: -</div>'
            else:
                comp_ratio, viol_mins = analyze_violation(df_all[metric], min_t, max_t)
                avg_val    = round(series.mean(), 1)
                latest_val = round(series.iloc[-1], 1)
                viol_color = c_red if viol_mins > 0 else c_light

                ratio_html  = f'<div style="color:{c_white}; font-size:36px; font-weight:bold; margin-bottom:15px;">준수율 {comp_ratio}%</div>'
                bottom_html = (
                    f'<div style="display:flex; justify-content:space-around; font-size:17px; color:{c_light}; font-weight:bold;">'
                    f'<div>평균: {avg_val}</div>'
                    f'<div style="color:{viol_color}">이탈: {viol_mins}분</div>'
                    f'</div>'
                )
                in_zone      = min_t <= latest_val <= max_t
                signal_color = '#00DD88' if in_zone else c_red
                signal_icon  = '🟢' if in_zone else '🔴'
                signal_text  = 'Safe Zone 이내' if in_zone else 'Safe Zone 이탈!'
                signal_html  = (
                    f'<div style="margin-top:14px; padding:10px 14px; border-radius:8px; '
                    f'background-color:{DAEDONG["black"]}; border:1px solid {signal_color};">'
                    f'<div style="font-size:22px; font-weight:bold; color:{signal_color};">'
                    f'{signal_icon} 현재값 {latest_val}</div>'
                    f'<div style="font-size:18px; font-weight:bold; color:{signal_color}; margin-top:4px;">'
                    f'{signal_text}</div>'
                    f'<div style="font-size:14px; color:{c_medium}; margin-top:6px;">'
                    f'기준 범위: {min_t} ~ {max_t}</div>'
                    f'</div>'
                )

            card_html = (
                f'<div style="background-color:{c_dark}; padding:25px 20px; border-radius:8px; '
                f'text-align:center; border:{border_width} solid {border_color};">'
                f'<div style="color:{c_light}; font-size:18px; font-weight:bold; margin-bottom:12px;">{metric}</div>'
                f'{ratio_html}{bottom_html}{signal_html}'
                f'</div>'
            )

            with cols[col_idx]:
                st.markdown(card_html, unsafe_allow_html=True)
                btn_label = "🔍 포커스 해제" if is_focused else "🔍 이 지표만 보기"
                if st.button(btn_label, key=f"btn_{metric}", use_container_width=True):
                    st.session_state["focused_metric"] = None if is_focused else metric
                    st.rerun()
            col_idx += 1

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # 차트: 축 겹침 방지 + 포커스 모드
    # ==========================================
    fig         = go.Figure()
    layout_axes = {}
    annotations = []
    chart_idx   = 0

    focused       = st.session_state.get("focused_metric", None)
    is_focus_mode = focused is not None
    x_start       = df_all['xdatetime'].min()
    x_end         = df_all['xdatetime'].max()

    # ✅ 축 겹침 방지: 지표 수에 따라 x축 domain과 y축 position 동적 계산
    n_metrics = len([m for m in selected_metrics
                     if m in df_all.columns
                     and not (df_all[m].dropna() == 0).all()
                     and len(df_all[m].dropna()) > 0])
    if is_focus_mode:
        n_metrics = 1

    # x축 오른쪽 여백: 우측 y축 개수만큼 확보
    right_axes = max(0, n_metrics - 1)
    x_domain_end = 1.0 - right_axes * 0.08  # 축당 8% 여백
    x_domain_end = max(0.6, x_domain_end)

    # 우측 y축 position 계산 (겹치지 않게 간격 부여)
    right_axis_positions = []
    for k in range(right_axes):
        right_axis_positions.append(round(x_domain_end + 0.01 + k * 0.09, 2))

    for i, metric in enumerate(selected_metrics):
        if metric not in df_all.columns:
            continue
        series = df_all[metric].dropna()
        if len(series) == 0 or (series == 0).all():
            continue

        if is_focus_mode and metric != focused:
            continue

        data_color   = LINE_COLORS.get(metric, '#DCDCD7')
        axis_name    = 'y' if (is_focus_mode or chart_idx == 0) else f'y{chart_idx + 1}'
        min_t, max_t = thresholds[metric]

        fig.add_trace(go.Scatter(
            x=df_all['xdatetime'], y=df_all[metric],
            mode='lines+markers', name=metric,
            line=dict(color=data_color, width=2.5),
            marker=dict(size=4),
            yaxis=axis_name,
            showlegend=True,
        ))

        if is_focus_mode:
            tc          = THRESHOLD_COLORS.get(metric, {'upper': '#FFD700', 'lower': '#FF8C00'})
            upper_color = tc['upper']
            lower_color = tc['lower']
            fig.add_trace(go.Scatter(
                x=[x_start, x_end], y=[max_t, max_t],
                mode='lines', name=f'↑ 상한 {max_t}',
                line=dict(color=upper_color, width=2.5, dash='dash'),
                yaxis=axis_name, showlegend=True,
            ))
            fig.add_trace(go.Scatter(
                x=[x_start, x_end], y=[min_t, min_t],
                mode='lines', name=f'↓ 하한 {min_t}',
                line=dict(color=lower_color, width=2.5, dash='dot'),
                yaxis=axis_name, showlegend=True,
            ))
            annotations.append(dict(
                x=x_end, y=max_t, xref='x', yref=axis_name,
                text=f'<b>상한 {max_t}</b>', showarrow=False,
                xanchor='left', yanchor='bottom',
                font=dict(color=upper_color, size=12),
                bgcolor=DAEDONG['black'],
            ))
            annotations.append(dict(
                x=x_end, y=min_t, xref='x', yref=axis_name,
                text=f'<b>하한 {min_t}</b>', showarrow=False,
                xanchor='left', yanchor='top',
                font=dict(color=lower_color, size=12),
                bgcolor=DAEDONG['black'],
            ))

        # ✅ 축 설정 — 겹침 방지 position 적용
        title_font = dict(size=14, color=data_color)
        tick_font  = dict(size=12, color=data_color)

        if chart_idx == 0:
            layout_axes['yaxis'] = dict(
                title=dict(text=f"<b>{metric}</b>", font=title_font),
                showgrid=True, gridcolor=DAEDONG['dark_gray'],
                tickfont=tick_font,
            )
        elif chart_idx == 1:
            pos = right_axis_positions[0] if right_axis_positions else 1.0
            layout_axes['yaxis2'] = dict(
                title=dict(text=f"<b>{metric}</b>", font=title_font),
                overlaying='y', side='right', showgrid=False,
                tickfont=tick_font,
                position=pos, anchor='free',
            )
        elif chart_idx == 2:
            pos = right_axis_positions[1] if len(right_axis_positions) > 1 else 1.0
            layout_axes['yaxis3'] = dict(
                title=dict(text=f"<b>{metric}</b>", font=title_font),
                overlaying='y', side='right', showgrid=False,
                tickfont=tick_font,
                position=pos, anchor='free',
            )

        chart_idx += 1

    fig.update_layout(
        paper_bgcolor=DAEDONG['black'], plot_bgcolor=DAEDONG['black'],
        font=dict(color=DAEDONG['light_gray'], size=13),
        xaxis=dict(
            title=dict(text="<b>측정 시각</b>", font=dict(size=15, color=DAEDONG['white'])),
            tickfont=dict(size=12), showgrid=True, gridcolor=DAEDONG['dark_gray'],
            domain=[0, x_domain_end],
        ),
        legend=dict(
            font=dict(size=12, color=DAEDONG['white']),
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            bgcolor="rgba(0,0,0,0)"
        ),
        annotations=annotations,
        margin=dict(l=20, r=int(right_axes * 90) + 40, t=60, b=20),
        height=500, **layout_axes
    )
    st.plotly_chart(fig, use_container_width=True)

# ==========================================
# 7. 하단 종합 분석 리포트
# ==========================================
st.markdown(f"<h3 style='color: {DAEDONG['white']}; margin-top:30px;'>📋 종합 환경 이탈 진단 리포트 ({period} / {zone_info['greenhouse']} {zone_info['name']})</h3>", unsafe_allow_html=True)
st.markdown(f"<div style='color: {DAEDONG['medium_gray']}; font-size:16px; margin-bottom:15px;'>현재 조회된 기간의 평균값을 우수농가 가이드라인과 비교하여 편차를 보여줍니다.</div>", unsafe_allow_html=True)

actual = {
    'avg_t':   df_all['내부온도(xintemp1)'].mean(),
    'day_t':   df_all[df_all['주야간(xjuya)'] == 1.0]['내부온도(xintemp1)'].mean(),
    'night_t': df_all[df_all['주야간(xjuya)'] == 0.0]['내부온도(xintemp1)'].mean(),
    'hum':     df_all['내부습도(xinhum1)'].mean(),
    'co2':     df_all['CO2농도(xco2)'].mean(),
}

if not df_all['누적일사량(xsunadd)'].isnull().all():
    daily_sun = df_all.groupby(df_all['xdatetime'].dt.date)['누적일사량(xsunadd)'].max()
    actual['sun'] = daily_sun.mean()
else:
    actual['sun'] = np.nan

actual['diff_t'] = (
    actual['night_t'] - actual['day_t']
    if not pd.isna(actual['night_t']) and not pd.isna(actual['day_t'])
    else np.nan
)

co2_series = df_all['CO2농도(xco2)'].dropna()
if len(co2_series) == 0 or (co2_series == 0).all():
    actual['co2'] = np.nan

# 하단 리포트는 선택된 가이드 월 기준으로 비교
report_guide = GUIDE_DATA.get(selected_guide_month, GUIDE_DATA[current_month])

def render_summary_card(title, act_val, guide_val, unit):
    if pd.isna(act_val):
        act_str, diff_str, diff_color = "데이터 없음", "-", DAEDONG['medium_gray']
    else:
        act_str = f"{act_val:.1f}{unit}" if unit != "J" else f"{act_val:.0f}{unit}"
        diff = act_val - guide_val
        if abs(diff) < 0.1:
            diff_str, diff_color = "목표 달성", DAEDONG['future_green_light']
        elif diff > 0:
            diff_str, diff_color = f"▲ +{diff:.1f}{unit}", DAEDONG['red']
        else:
            diff_str, diff_color = f"▼ {abs(diff):.1f}{unit}", DAEDONG['blue']
    return f"""
    <div style="background-color: {DAEDONG['dark_gray']}; padding: 18px; border-radius: 8px;
                border-top: 4px solid {DAEDONG['future_green']}; text-align: center;">
        <div style="color: {DAEDONG['light_gray']}; font-size: 16px; font-weight: bold; margin-bottom: 8px;">{title}</div>
        <div style="color: {DAEDONG['white']}; font-size: 26px; font-weight: bold; margin-bottom: 8px;">{act_str}</div>
        <div style="font-size: 14px; color: {DAEDONG['medium_gray']}; margin-bottom: 10px;">가이드({selected_guide_month}월): {guide_val}{unit}</div>
        <div style="font-size: 16px; font-weight: bold; color: {diff_color};
                    background-color: {DAEDONG['black']}; padding: 6px; border-radius: 4px;">{diff_str}</div>
    </div>
    """

col1, col2, col3, col4 = st.columns(4)
col1.markdown(render_summary_card("평균온도",  actual['avg_t'],   report_guide['avg_t'],   "℃"), unsafe_allow_html=True)
col2.markdown(render_summary_card("주간온도",  actual['day_t'],   report_guide['day_t'],   "℃"), unsafe_allow_html=True)
col3.markdown(render_summary_card("야간온도",  actual['night_t'], report_guide['night_t'], "℃"), unsafe_allow_html=True)
col4.markdown(render_summary_card("주야간차",  actual['diff_t'],  report_guide['diff_t'],  "℃"), unsafe_allow_html=True)

st.markdown("<div style='height: 15px;'></div>", unsafe_allow_html=True)

col5, col6, col7, col8 = st.columns(4)
col5.markdown(render_summary_card("평균습도",      actual['hum'], report_guide['hum'], "%"),   unsafe_allow_html=True)
col6.markdown(render_summary_card("평균 CO2",      actual['co2'], report_guide['co2'], "ppm"), unsafe_allow_html=True)
col7.markdown(render_summary_card("누적일사량(일)", actual['sun'], report_guide['sun'], "J"),   unsafe_allow_html=True)
col8.markdown(f"<div style='background-color: transparent; padding: 15px;'></div>", unsafe_allow_html=True)

if auto_refresh:
    time.sleep(60)
    st.rerun()
