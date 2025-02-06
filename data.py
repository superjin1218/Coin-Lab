import pandas as pd
import numpy as np

# 파일 경로
bitcoin_file_path = 'bitcoin_price_data.csv'
sp500_file_path = 'S&P.csv'

# 데이터 불러오기
bitcoin_data = pd.read_csv(bitcoin_file_path)
sp500_data = pd.read_csv(sp500_file_path)

# 날짜 컬럼을 datetime 형식으로 변환
bitcoin_data['날짜'] = pd.to_datetime(bitcoin_data['날짜'])
sp500_data['날짜'] = pd.to_datetime(sp500_data['날짜'])

# 종가 컬럼을 숫자로 변환 (콤마 제거 필요 시 적용)
bitcoin_data['종가'] = pd.to_numeric(bitcoin_data['종가'], errors='coerce')
sp500_data['종가'] = pd.to_numeric(sp500_data['종가'].str.replace(',', ''), errors='coerce')

# 날짜를 기준으로 정렬
bitcoin_data = bitcoin_data.sort_values('날짜').reset_index(drop=True)
sp500_data = sp500_data.sort_values('날짜').reset_index(drop=True)

# 일일 수익률 계산
bitcoin_data['daily_return'] = bitcoin_data['종가'].pct_change()
sp500_data['daily_return'] = sp500_data['종가'].pct_change()

# NaN 제거
bitcoin_data = bitcoin_data.dropna(subset=['daily_return'])
sp500_data = sp500_data.dropna(subset=['daily_return'])

# 연도별 변동성 계산 함수
def calculate_annualized_volatility_by_year(data, trading_days):
    data['year'] = data['날짜'].dt.year  # 연도별 그룹화
    annual_volatility = data.groupby('year')['daily_return'].std() * np.sqrt(trading_days)
    return annual_volatility

# 비트코인: 365일 기준
btc_annual_volatility = calculate_annualized_volatility_by_year(bitcoin_data, 365)

# S&P 500: 252일 기준
sp500_annual_volatility = calculate_annualized_volatility_by_year(sp500_data, 252)

# 결과 출력
print("비트코인의 연도별 연간 변동성:")
print(btc_annual_volatility)
print("\nS&P 500의 연도별 연간 변동성:")
print(sp500_annual_volatility)

# 전체 평균 연간 변동성 계산
btc_avg_volatility = btc_annual_volatility.mean()
sp500_avg_volatility = sp500_annual_volatility.mean()

print(f"\n비트코인의 평균 연간 변동성 (6년 기준): {btc_avg_volatility:.2%}")
print(f"S&P 500의 평균 연간 변동성 (6년 기준): {sp500_avg_volatility:.2%}")
