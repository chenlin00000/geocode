import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import os

# 使用Nominatim进行地理编码，并设置超时时间为10秒
geolocator = Nominatim(user_agent="geoapiExercises", timeout=10)

# 设置RateLimiter，防止因请求太频繁被服务器拒绝，这里设置每次请求之间的间隔为2秒
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)

# 读取数据
df = pd.read_csv('data.csv')

# 检查输出文件是否存在，如果存在，找到最后处理的行
start_row = 0
if os.path.exists('result.csv'):
    df_result = pd.read_csv('result.csv')
    if not df_result.empty:
        start_row = df_result.index[-1] + 1

# 对于每一行数据
for i, intro in enumerate(df['intro'].iloc[start_row:]):
    i += start_row
    # 进行地理编码并存储结果
    location = geocode(intro)
    if location:
        df.at[i, 'latitude'] = location.latitude
        df.at[i, 'longitude'] = location.longitude

    # 每十行打印一次正在处理的行数，并保存数据
    if i % 10 == 0:
        print(f"Processing row {i+1}/{len(df)}")
        df.to_csv('result.csv', index=False)
