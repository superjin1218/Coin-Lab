import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import requests
import time

# Upbit API에서 캔들 데이터를 가져오는 함수
def fetch_candles_bulk(market, count, to=None):
    url = f"https://api.upbit.com/v1/candles/days"
    params = {"market": market, "count": count}
    if to:
        params["to"] = to
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"API 요청 실패: 상태 코드 {response.status_code}, 메시지: {response.text}")
        return []

# 데이터를 가져와 DataFrame으로 변환
def load_data_from_upbit(market, days):
    candles = []
    to_date = None

    while len(candles) < days:
        batch = fetch_candles_bulk(market, min(200, days - len(candles)), to=to_date)
        if not batch:
            break
        candles.extend(batch)
        to_date = batch[-1]["candle_date_time_utc"]
        time.sleep(0.5)  # API 요청 간격 조정

    candles.reverse()  # 시간 순서 정렬
    data = {
        "날짜": [datetime.strptime(candle["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S") for candle in candles],
        "종가": [candle["trade_price"] for candle in candles],
        "시가": [candle["opening_price"] for candle in candles],
        "저가": [candle["low_price"] for candle in candles],
        "거래량": [candle["candle_acc_trade_volume"] for candle in candles],
    }
    return pd.DataFrame(data)

# 이동평균 계산 함수
def calculate_moving_average(prices, window):
    series = pd.Series(prices)
    return series.shift(1).rolling(window=window).mean().tolist()

# 이동평균 및 거래량 평균 계산
def calculate_moving_averages(df):
    df['MA_20'] = calculate_moving_average(df['종가'], 20)
    df['MA_50'] = calculate_moving_average(df['종가'], 50)
    df['MA_120'] = calculate_moving_average(df['종가'], 120)
    df['MA_200'] = calculate_moving_average(df['종가'], 200)
    return df

def calculate_rolling_volume_average(df):
    rolling_volume = []
    current_avg = None

    for i in range(len(df)):
        today_volume = df.iloc[i]['거래량']

        if i < 365:
            # 365일 되기 전까지는 현재까지의 평균 계산
            if current_avg is None:
                current_avg = today_volume
            else:
                current_avg = (current_avg * i + today_volume) / (i + 1)
        else:
            # 365일 이후부터는 이전 평균에서 오래된 값 빼고 새 값 추가
            old_volume = df.iloc[i - 365]['거래량']
            current_avg = current_avg + (today_volume - old_volume) / 365

        rolling_volume.append(current_avg)

    df['Volume_Avg_365'] = rolling_volume
    return df

# 과거 최고가 계산 함수
def calculate_all_time_high(df):
    all_time_highs = []
    current_high = float('-inf')
    for price in df['종가']:
        current_high = max(current_high, price)
        all_time_highs.append(current_high)
    df['All_Time_High'] = all_time_highs
    return df

# 수익률 계산 함수
def calculate_returns(df, date_indices, periods):
    returns = {period: [] for period in periods}

    for date_idx in date_indices:
        start_price = df.iloc[date_idx]['종가']
        for period in periods:
            future_idx = date_idx + period
            if future_idx < len(df):
                future_price = df.iloc[future_idx]['종가']
                returns[period].append((future_price - start_price) / start_price * 100)
            else:
                returns[period].append(None)

    return returns

# 승률 계산 함수
def calculate_win_rate(returns):
    win_rates = {}
    for period, values in returns.items():
        valid_values = [value for value in values if value is not None]
        wins = [value for value in valid_values if value > 0]
        win_rate = len(wins) / len(valid_values) * 100 if valid_values else 0
        win_rates[period] = win_rate
    return win_rates

# 데이터 분석 함수
def analyze_conditions(df):
    # 이동평균선 및 거래량 계산
    df = calculate_moving_averages(df)
    df = calculate_rolling_volume_average(df)

    # 과거 최고가 계산
    df = calculate_all_time_high(df)

    # 결과 저장
    results = {
        'Golden_Cross_200': [],
        'FTD_Above_200': [],
        'High_Volume_2x_above_200': [],
        'MA_120_Touch_above_200': [],
        'High_Volume_3x': [],
        'Sequential_FTD_above_200': [],
        'Sequential_High_Volume_2x_above_200': [],
        'Sequential_MA_120_Touch_above_200': []
    }

    for i in range(200, len(df) - 180):
        row = df.iloc[i]

        # Golden Cross 200 조건 확인
        if row['MA_200'] is not None and df.iloc[i - 1]['종가'] < df.iloc[i - 1]['MA_200'] and min(row['시가'], row['종가']) <= row['MA_200'] <= max(row['시가'], row['종가']):
            results['Golden_Cross_200'].append(i)

        # FTD Above 200 조건 확인
        if row['MA_200'] is not None and row['종가'] > row['MA_200'] and row['종가'] < row['All_Time_High'] * 0.7:
            if row['종가'] >= row['시가'] * 1.0572:
                valid = True
                for j in range(1, 4):
                    future_row = df.iloc[i + j]
                    if future_row['저가'] < row['저가']:
                        valid = False
                        break

                if valid:
                    for k in range(4, 8):
                        future_row = df.iloc[i + k]
                        if future_row['종가'] >= row['시가'] * 1.0572 and future_row['저가'] >= row['저가']:
                            volume_increasing = all(
                                df.iloc[i + n]['거래량'] > df.iloc[i + n - 1]['거래량']
                                for n in range(4, k + 1)
                            )
                            if volume_increasing:
                                results['FTD_Above_200'].append(i)
                                break

        # High Volume 2x Above 200 조건 확인
        if row['MA_200'] is not None and row['종가'] > row['MA_200'] and row['거래량'] > row['Volume_Avg_365'] * 2:
            results['High_Volume_2x_above_200'].append(i)

        # MA 120 Touch Above 200 조건 확인 (주가가 200일선 위이고 종가와 시가 사이에 120일선 위치)
        if row['MA_120'] is not None and row['MA_200'] is not None and row['종가'] > row['MA_200'] and row['MA_120'] >= min(row['종가'], row['시가']) and row['MA_120'] <= max(row['종가'], row['시가']):
            results['MA_120_Touch_above_200'].append(i)

        # High Volume 3x 조건 확인 (200일선 조건 없음)
        if row['거래량'] > row['Volume_Avg_365'] * 3:
            results['High_Volume_3x'].append(i)

        # Sequential FTD Above 200 조건 확인 (정배열 조건 수정)
        if (
            row['MA_20'] is not None and row['MA_50'] is not None and
            row['MA_120'] is not None and row['MA_200'] is not None and
            row['MA_20'] > row['MA_50'] > row['MA_120'] > row['MA_200'] and  # 정배열 수정
            row['종가'] > row['MA_200'] and row['종가'] < row['All_Time_High'] * 0.7 and
            row['종가'] >= row['시가'] * 1.0572
        ):
            valid = True
            for j in range(1, 4):
                future_row = df.iloc[i + j]
                if future_row['저가'] < row['저가']:
                    valid = False
                    break

            if valid:
                for k in range(4, 8):
                    future_row = df.iloc[i + k]
                    if future_row['종가'] >= row['시가'] * 1.0572 and future_row['저가'] >= row['저가']:
                        volume_increasing = all(
                            df.iloc[i + n]['거래량'] > df.iloc[i + n - 1]['거래량']
                            for n in range(4, k + 1)
                        )
                        if volume_increasing:
                            results['Sequential_FTD_above_200'].append(i)
                            break

        # Sequential High Volume 2x Above 200 조건 확인
        if (
            row['MA_20'] is not None and row['MA_50'] is not None and
            row['MA_120'] is not None and row['MA_200'] is not None and
            row['MA_20'] > row['MA_50'] > row['MA_120'] > row['MA_200'] and  # 정배열 수정
            row['거래량'] > row['Volume_Avg_365'] * 2
        ):
            results['Sequential_High_Volume_2x_above_200'].append(i)

        # Sequential MA 120 Touch Above 200 조건 확인
        if (
            row['MA_20'] is not None and row['MA_50'] is not None and
            row['MA_120'] is not None and row['MA_200'] is not None and
            row['MA_20'] > row['MA_50'] > row['MA_120'] > row['MA_200'] and  # 정배열 수정
            row['MA_120'] >= min(row['종가'], row['시가']) and
            row['MA_120'] <= max(row['종가'], row['시가'])
        ):
            results['Sequential_MA_120_Touch_above_200'].append(i)

    return results

# 결과 출력 및 시각화
def print_and_visualize_results(df, results):
    periods = [30, 90, 180]  # 1개월, 3개월, 6개월
    all_returns = {}

    for condition, indices in results.items():
        returns = calculate_returns(df, indices, periods)
        all_returns[condition] = returns
        win_rates = calculate_win_rate(returns)

        print(f"{condition} 승률:")
        print(f"1M: {win_rates[30]:.2f}%, 3M: {win_rates[90]:.2f}%, 6M: {win_rates[180]:.2f}%")
        print(f"{condition} 날짜:")
        print([df.iloc[idx]['날짜'].strftime('%Y-%m-%d') for idx in indices])

    # 시각화
    for period in periods:
        plt.figure(figsize=(12, 8))
        for condition, returns in all_returns.items():
            values = returns[period]
            plt.scatter([condition] * len(values), values, label=condition, alpha=0.7)
        plt.axhline(0, color="red", linestyle="--")
        plt.title(f"{period // 30}M Returns Comparison")
        plt.ylabel("Return (%)")
        plt.xlabel("Conditions")
        plt.legend()
        plt.show()

# 실행
market = "KRW-BTC"
data = load_data_from_upbit(market, 2000)

if data is not None:
    analysis_results = analyze_conditions(data)
    print_and_visualize_results(data, analysis_results)


