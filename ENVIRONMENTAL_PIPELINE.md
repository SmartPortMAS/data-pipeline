# 환경 데이터 파이프라인 — 조위 · 파고 · 항만기상

울산항 해양 환경 데이터(조위, 파고, 항만기상) 실시간 수집 및 전처리 파이프라인.

---

## 목차

1. [목적](#1-목적)
2. [전체 흐름](#2-전체-흐름)
3. [디렉토리 구조](#3-디렉토리-구조)
4. [환경 설정](#4-환경-설정)
5. [조위 파이프라인](#5-조위-파이프라인)
6. [파고 파이프라인](#6-파고-파이프라인)
7. [항만기상 파이프라인](#7-항만기상-파이프라인)
8. [공통 전처리 모듈](#8-공통-전처리-모듈-common_preprocessingpy)
9. [품질 플래그](#9-품질-플래그-quality_flag)
10. [실행 순서 요약](#10-실행-순서-요약)

---

## 1. 목적

항만 운영 안전성 평가와 LLM 기반 종합 보고서 생성에 필요한 실시간 해양 환경 데이터를 수집·정제하여 Staging CSV로 적재한다.

| 데이터 | 활용 목적 |
|---|---|
| 조위 | 접안 가능 수심 판단, 선박 출입항 시점 결정 |
| 파고 | 하역 작업 가능 여부 판단 (유의파고 기준) |
| 항만기상 | 풍속·시정에 따른 하역 중단 기준 판단 |

---

## 2. 전체 흐름

세 파이프라인 모두 동일한 2단계 구조를 따른다.

```
[외부 API]
    │
    ▼  collectors/  (Step 1: 수집)
[data/raw/<도메인>/<도메인>_obs_YYYYMMDD_raw.json]
    │
    ▼  preprocessors/  (Step 2: 전처리)
[data/staging/<도메인>_obs_stg.csv]
```

```
조위:     tide_collector.py   →  tide_obs_YYYYMMDD_raw.json   →  tide_obs_stg.csv
파고:     wave_collector.py   →  wave_obs_YYYYMMDD_raw.json   →  wave_obs_stg.csv
항만기상: weather_collector.py → weather_obs_YYYYMMDD_raw.json → weather_obs_stg.csv
```

---

## 3. 디렉토리 구조

```
smart-port-multi-agent/
├── .env                          # API 키 (git 제외)
│
├── data/
│   ├── raw/
│   │   ├── tide/
│   │   │   └── tide_obs_YYYYMMDD_raw.json
│   │   ├── wave/
│   │   │   └── wave_obs_YYYYMMDD_raw.json
│   │   └── weather/
│   │       └── weather_obs_YYYYMMDD_raw.json
│   └── staging/
│       ├── tide_obs_stg.csv
│       ├── wave_obs_stg.csv
│       └── weather_obs_stg.csv
│
└── data_pipeline/
    ├── requirements.txt
    ├── common_preprocessing.py       # 팀 공통 유틸 (결측 처리, 날짜 변환 등)
    │
    ├── collectors/
    │   ├── tide_collector.py         # 조위 수집 (국립해양조사원)
    │   ├── wave_collector.py         # 파고 수집 (기상청 BUOY)
    │   └── weather_collector.py      # 항만기상 수집 (해양수산부)
    │
    └── preprocessors/
        ├── tide_preprocessor.py      # 조위 전처리
        ├── wave_preprocessor.py      # 파고 전처리
        └── weather_preprocessor.py   # 항만기상 전처리
```

---

## 4. 환경 설정

### 패키지 설치

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r data_pipeline/requirements.txt
```

### .env 파일

`.env.example`을 복사하여 `.env`를 만들고 아래 항목을 채운다.

```env
# 조위 (국립해양조사원 공공데이터포탈)
KHOA_API_KEY=디코딩된_API_키

# 파고 (기상청 APIHUB)
KMA_BUOY_AUTH_KEY=인증키

# 항만기상 (해양수산부 항만기상정보시스템)
MMAF_API_KEY=디코딩된_API_키
```

> API 키는 모두 **URL 디코딩된 값**을 입력한다. 포탈에서 복사한 `%XX` 형식 값 그대로 넣으면 이중 인코딩 오류가 발생한다.

---

## 5. 조위 파이프라인

### 5-1. 데이터 출처

| 항목 | 내용 |
|---|---|
| 기관 | 국립해양조사원 (KHOA) |
| API | 공공데이터포탈 `GetDTRecentApiService` |
| 관측소 | 울산 조위관측소 (`DT_0020`) |
| 좌표 | 위도 35.50194 / 경도 129.38722 |
| 수집 주기 | 10분 간격 |

### 5-2. 수집기 (`tide_collector.py`)

#### 동작 방식

```
KHOA API 호출 (obsCode=DT_0020, min=10)
  → items 리스트 수신
  → 관측소 메타데이터(_obs_code, _latitude 등) 주입
  → obsrvnDt 기준 중복 제거 후 당일 파일에 누적 저장
```

#### 수집 항목

| API 필드명 | 의미 |
|---|---|
| `obsrvnDt` | 관측일시 |
| `bscTdlvHgt` | 조위 (기본수준면 기준, cm) |
| `wndrct` | 풍향 (deg) |
| `wspd` | 풍속 (m/s) |
| `maxMmntWspd` | 최대순간풍속 (m/s) |
| `artmp` | 기온 (℃) |
| `atmpr` | 기압 (hPa) |
| `wtem` | 수온 (℃) |
| `slntQty` | 염분 (psu) |
| `crdir` | 유향 (deg) |
| `crsp` | 유속 (cm/s) |

#### raw JSON 구조

```json
{
  "collected_at_utc": "2025-06-01T03:00:00Z",
  "source": "KDPA_KHOA_dtRecent",
  "query_date_kst": "20250601",
  "station": {
    "obs_code": "DT_0020",
    "obs_name": "울산",
    "latitude": 35.50194,
    "longitude": 129.38722
  },
  "record_count": 144,
  "is_synthetic": false,
  "data": [
    {
      "obsrvnDt": "202506010000",
      "bscTdlvHgt": "120",
      "wndrct": "270",
      "wspd": "3.5",
      "_obs_code": "DT_0020",
      "_obs_name": "울산",
      "_latitude": 35.50194,
      "_longitude": 129.38722
    }
  ]
}
```

#### 실행

```bash
python -m data_pipeline.collectors.tide_collector
```

---

### 5-3. 전처리기 (`tide_preprocessor.py`)

#### 처리 순서

```
1. data/raw/tide/*.json 전체 로드
2. API 필드명 → 표준 컬럼명 변환 (COLUMN_MAP)
3. 결측값 정규화 ("null", "-", "" → NaN)
4. 숫자형 변환 (조위·풍속·기온 등)
5. KST → UTC 변환 (obsrvnDt: YYYYMMDDHHMM 형식)
6. 공통 메타데이터 추가 (source_system, quality_flag 등)
7. 기본키 결측 체크 (station_id, observed_at_utc)
8. 값 범위 검증 (validate_tide)
9. 중복 제거 (station_id + observed_at_utc 복합키)
10. data/staging/tide_obs_stg.csv 저장
```

#### 컬럼 매핑표

| API 필드 | staging 컬럼 | 단위 |
|---|---|---|
| `_obs_code` | `station_id` | — |
| `_obs_name` | `station_name` | — |
| `obsrvnDt` | `observed_at_utc` | UTC 변환 |
| `bscTdlvHgt` | `tide_level_cm` | cm |
| `wndrct` | `wind_dir_deg` | deg |
| `wspd` | `wind_speed_ms` | m/s |
| `maxMmntWspd` | `gust_ms` | m/s |
| `artmp` | `air_temp_c` | ℃ |
| `atmpr` | `air_pressure_hpa` | hPa |
| `wtem` | `sea_temp_c` | ℃ |
| `slntQty` | `salinity_psu` | psu |
| `crdir` | `current_dir_deg` | deg |
| `crsp` | `current_speed_cms` | cm/s |

#### 값 범위 검증 기준

| 컬럼 | 정상 범위 | 이상 시 플래그 |
|---|---|---|
| `tide_level_cm` | -50 ~ 500 | `INVALID_TIDE_LEVEL` |
| `wind_speed_ms` | ≥ 0 | `INVALID_WIND_SPEED` |
| `wind_dir_deg` | 0 ~ 360 | `INVALID_WIND_DIR` |

#### 실행

```bash
python -m data_pipeline.preprocessors.tide_preprocessor
```

---

## 6. 파고 파이프라인

### 6-1. 데이터 출처

| 항목 | 내용 |
|---|---|
| 기관 | 기상청 (KMA) |
| API | KMA APIHUB BUOY API (`kma_buoy.php`) |
| 관측소 | 울산 부이 (STN `22189`) |
| 수집 주기 | 30분 간격 |

### 6-2. 수집기 (`wave_collector.py`)

#### 동작 방식

```
KMA APIHUB 호출 (stn=22189)
  → 텍스트 응답 파싱 (공백 구분 고정 컬럼 순서)
  → -99/-99.0 등 결측 마커 → None으로 변환
  → tm_kst 기준 중복 제거 후 당일 파일에 누적 저장
```

#### 응답 형식 및 컬럼 순서 (17개)

| 순서 | 컬럼명 | 의미 |
|---|---|---|
| 1 | `tm_kst` | 관측시각 (YYYYMMDDHHMI) |
| 2 | `stn_id` | 지점 번호 |
| 3 | `wind_dir1_deg` | 풍향1 (deg) |
| 4 | `wind_speed1_ms` | 풍속1 (m/s) |
| 5 | `gust1_ms` | 돌풍1 (m/s) |
| 6 | `wind_dir2_deg` | 풍향2 (deg) |
| 7 | `wind_speed2_ms` | 풍속2 (m/s) |
| 8 | `gust2_ms` | 돌풍2 (m/s) |
| 9 | `air_pressure_hpa` | 기압 (hPa) |
| 10 | `humidity_pct` | 습도 (%) |
| 11 | `air_temp_c` | 기온 (℃) |
| 12 | `sea_temp_c` | 해수온도 (℃) |
| 13 | `wave_height_max_m` | 최대파고 (m) |
| 14 | `wave_height_sig_m` | 유의파고 (m) |
| 15 | `wave_height_avg_m` | 평균파고 (m) |
| 16 | `wave_period_s` | 파주기 (s) |
| 17 | `wave_dir_deg` | 파향 (deg) |

> **유의파고(wave_height_sig_m)**: 전체 파의 상위 1/3 평균값. 해상 위험도 기준으로 가장 많이 쓰인다.

#### 실행

```bash
python -m data_pipeline.collectors.wave_collector
```

---

### 6-3. 전처리기 (`wave_preprocessor.py`)

#### 처리 순서

```
1. data/raw/wave/*.json 전체 로드
   → station 메타데이터(station_id, station_name)를 각 레코드에 주입
2. 결측값 정규화
3. 숫자형 변환 (파고·파주기·파향·풍속 등 15개 컬럼)
4. KST → UTC 변환 (YYYYMMDDHHMI 12자리)
5. 공통 메타데이터 추가
6. 기본키 결측 체크 (station_id, observed_at_utc)
7. 값 범위 검증 (validate_wave)
8. 중복 제거 (station_id + observed_at_utc 복합키)
9. data/staging/wave_obs_stg.csv 저장
```

#### 값 범위 검증 기준

| 컬럼 | 정상 범위 | 이상 시 플래그 |
|---|---|---|
| `wave_height_sig_m` | 0 ~ 20 | `INVALID_WAVE_HEIGHT` |
| `wave_dir_deg` | 0 ~ 360 | `INVALID_WAVE_DIR` |
| `wind_speed1_ms` | ≥ 0 | `INVALID_WIND_SPEED` |

#### 실행

```bash
python -m data_pipeline.preprocessors.wave_preprocessor
```

---

## 7. 항만기상 파이프라인

### 7-1. 데이터 출처

| 항목 | 내용 |
|---|---|
| 기관 | 해양수산부 항만기상정보시스템 |
| API | `marineweather.nmpnt.go.kr/openWeatherNow.do` |
| 관측소 | 울산항동방파제서단등대 (MMSI: `1041519`) |
| 청 코드 | 울산청 (`104`) |
| 좌표 | 위도 35.4665 / 경도 129.399889 |
| 수집 주기 | 실시간 (매 호출마다 최신 1건) |

### 7-2. 수집기 (`weather_collector.py`)

#### 동작 방식

```
MMAF API 호출 (mmaf=104, mmsi=1041519, dataType=1)
  → result.status == "OK" 확인
  → recordset[0] (최신 1건) 추출
  → DATETIME 기준 당일 파일에 append 저장
```

#### 수집 항목

| API 필드명 | 의미 |
|---|---|
| `DATETIME` | 관측일시 (YYYYMMDDHHMMSS) |
| `WIND_DIRECT` | 풍향 (deg) |
| `WIND_SPEED` | 풍속 (m/s) |
| `AIR_TEMPERATURE` | 기온 (℃) |
| `HUMIDITY` | 습도 (%) |
| `AIR_PRESSURE` | 기압 (hPa) |
| `HORIZON_VISIBL` | 시정 (m) |

#### 실행

```bash
python -m data_pipeline.collectors.weather_collector
```

---

### 7-3. 전처리기 (`weather_preprocessor.py`)

#### 처리 순서

```
1. data/raw/weather/*.json 전체 로드
2. '데이터없음' / '미제공' / '' → NaN (API 특수 결측 마커 처리)
3. 컬럼명 표준화 (대문자 API 필드 → 소문자 표준명)
4. 결측값 정규화
5. 관측시각 파싱 (YYYYMMDDHHMMSS → UTC)
6. 숫자형 변환 (풍향·풍속·기온·습도·기압·시정·좌표)
7. 공통 메타데이터 추가
8. 기본키 결측 체크 (station_id, observed_at_utc)
9. 좌표 범위 검증 (울산 Bounding Box)
10. 풍향·풍속 범위 검증
11. 중복 제거 (station_id + observed_at_utc 복합키)
12. data/staging/weather_obs_stg.csv 저장
```

#### 컬럼 매핑표

| API 필드 | staging 컬럼 | 단위 |
|---|---|---|
| `MMSI_CODE` | `station_id` | — |
| `MMSI_NM` | `station_name` | — |
| `DATETIME` | `observed_at_utc` | UTC 변환 |
| `WIND_DIRECT` | `wind_dir_deg` | deg |
| `WIND_SPEED` | `wind_speed_ms` | m/s |
| `AIR_TEMPERATURE` | `air_temp_c` | ℃ |
| `HUMIDITY` | `humidity_pct` | % |
| `AIR_PRESSURE` | `air_pressure_hpa` | hPa |
| `HORIZON_VISIBL` | `visibility_m` | m |

#### 값 범위 검증 기준

| 검증 종류 | 기준 | 이상 시 플래그 |
|---|---|---|
| 풍향 범위 | 0 ~ 360 | `INVALID_WIND_DIR` |
| 풍속 범위 | ≥ 0 | `INVALID_WIND_SPEED` |
| 좌표 결측/0 | latitude 또는 longitude가 null/0 | `MISSING_COORDINATE` |
| 울산 영역 이탈 | 위도 35.30~35.58 / 경도 129.18~129.52 범위 밖 | `OUT_OF_ULSAN_BBOX` |

#### 실행

```bash
python -m data_pipeline.preprocessors.weather_preprocessor
```

---

## 8. 공통 전처리 모듈 (`common_preprocessing.py`)

세 파이프라인이 모두 공유하는 유틸리티 함수 모음.

| 함수 | 역할 |
|---|---|
| `normalize_nulls(df)` | `""`, `"null"`, `"-"`, `"N/A"` 등 → `NaN` |
| `standardize_column_names(df, column_map)` | API 필드명 → 표준 컬럼명 rename |
| `to_numeric_safe(df, columns)` | 지정 컬럼을 숫자로 변환, 변환 불가 값은 `NaN` |
| `parse_datetime_kst_to_utc(df, columns)` | KST 날짜 문자열 → UTC Timestamp |
| `add_common_metadata(df, ...)` | `source_system`, `quality_flag="OK"`, `collected_at_utc`, `is_synthetic` 추가 |
| `flag_missing_key(df, key_columns)` | 기본키 컬럼이 null이면 `quality_flag = "MISSING_KEY"` |
| `flag_ulsan_bbox(df)` | 좌표가 울산 영역 밖이면 `OUT_OF_ULSAN_BBOX` |
| `save_staging_csv(df, output_path)` | `utf-8-sig` 인코딩으로 CSV 저장 (Excel 호환) |

---

## 9. 품질 플래그 (`quality_flag`)

staging CSV의 모든 레코드에 `quality_flag` 컬럼이 포함된다.

| 값 | 의미 | 처리 권고 |
|---|---|---|
| `OK` | 정상 레코드 | 분석에 사용 |
| `MISSING_KEY` | 기본키(관측소 ID 또는 관측시각) 누락 | 제외 또는 별도 조사 |
| `INVALID_TIDE_LEVEL` | 조위가 -50 ~ 500cm 범위 밖 | 이상값으로 처리 |
| `INVALID_WAVE_HEIGHT` | 유의파고가 0 ~ 20m 범위 밖 | 이상값으로 처리 |
| `INVALID_WAVE_DIR` | 파향이 0 ~ 360° 범위 밖 | 이상값으로 처리 |
| `INVALID_WIND_DIR` | 풍향이 0 ~ 360° 범위 밖 | 이상값으로 처리 |
| `INVALID_WIND_SPEED` | 풍속이 음수 | 이상값으로 처리 |
| `MISSING_COORDINATE` | 위도 또는 경도가 null/0 | 좌표 기반 분석 제외 |
| `OUT_OF_ULSAN_BBOX` | 좌표가 울산 영역 밖 | 관측소 설정 오류 점검 |

---

## 10. 실행 순서 요약

```bash
.venv\Scripts\Activate.ps1

# ── 조위 ──
python -m data_pipeline.collectors.tide_collector
python -m data_pipeline.preprocessors.tide_preprocessor

# ── 파고 ──
python -m data_pipeline.collectors.wave_collector
python -m data_pipeline.preprocessors.wave_preprocessor

# ── 항만기상 ──
python -m data_pipeline.collectors.weather_collector
python -m data_pipeline.preprocessors.weather_preprocessor
```

### 재실행 안전성

| 파이프라인 | 중복 제거 기준 |
|---|---|
| 조위 | `obsrvnDt` 기준 중복 제거 후 파일에 누적 |
| 파고 | `tm_kst` 기준 중복 제거 후 파일에 누적 |
| 항만기상 | append 방식 — 전처리 단계 `drop_duplicates`로 staging에서 제거 |

---

## 향후 예정

```
완료
  ✅ 조위 실시간 수집 (국립해양조사원, 울산 DT_0020)
  ✅ 파고 실시간 수집 (기상청 BUOY, 울산 STN 22189)
  ✅ 항만기상 실시간 수집 (해양수산부, 울산항동방파제서단등대)
  ✅ 전처리 및 staging CSV 적재
  ✅ 공통 품질 플래그 체계

예정
  ⬜ PostgreSQL staging 테이블 적재
  ⬜ 시계열 이상 탐지 (이전 값 대비 급변 감지)
  ⬜ LLM 에이전트 컨텍스트 연동
  ⬜ 실시간 대시보드 시각화
```
