# Air Quality Data Scraper

대기질 측정소 정보와 실시간 대기질 측정 데이터를 수집하여 PostgreSQL 데이터베이스에 저장하는 스크립트입니다.

## 기능

- 측정소 정보 수집 및 저장
- 실시간 대기질 측정 데이터 수집 및 저장
- 7일 이상 된 데이터 자동 삭제
- 로깅 기능

## 설치 방법

1. 필요한 패키지 설치:
```bash
pip install -r requirements.txt
```

2. 환경 변수 설정:
`.env` 파일을 프로젝트 루트 디렉토리에 생성하고 다음 내용을 추가:
```env
# API Configuration
API_KEY=your_api_key
측정소_API_URL=http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList
실시간_측정소_정보_API_URL=http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty

# Database Configuration
DATABASE_URL=your_database_url
```

## 데이터베이스 스키마

### measuring_station 테이블
- 측정소 기본 정보 저장
- 컬럼: std_dt, stationname, addr, year, mangname, item, dmx, dmy

### measuring_station_realtime 테이블
- 실시간 대기질 측정 데이터 저장
- 컬럼: std_dt, station_name, data_Time, so2value, so2grade, covalue, cograde, o3value, o3grade, no2value, no2grade, pm10value, pm10grade, pm25value, pm25grade, khaivalue, khaigrade

## 사용 방법

스크립트 실행:
```bash
python air_quality_scraper.py
```

## 로깅

- 로그 파일: `air_quality.log`
- 로그 레벨: INFO
- 로그 포맷: 시간 - 로그레벨 - 메시지

## 주의사항

- API 키와 데이터베이스 접속 정보는 `.env` 파일에서 관리
- `.env` 파일은 버전 관리에서 제외 (`.gitignore`에 추가)
- 7일 이상 된 데이터는 자동으로 삭제됨

## 라이선스

이 프로젝트는 Apache Version 2.0 라이선스를 따릅니다. 