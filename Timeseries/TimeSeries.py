import pandas as pd
import matplotlib.pyplot as plt
from pmdarima.arima import auto_arima
from pmdarima.arima import ADFTest
from statsmodels.tsa.stattools import adfuller

df = pd.read_csv('./Timeseries/year_sales.csv')

df['Year'] = pd.to_datetime(df['Year'])
df.set_index('Year', inplace=True)

df = df.sort_index()

df.plot()
plt.title("Yearly Sales Data")
plt.show()

# Add-ons P-value #####################
# •	p-value ≤ alpha: อนุกรมเวลา สถานี
adf_result = adfuller(df['Sales']) 
print(f"ADF Statistic: {adf_result[0]}")
print(f"p-value: {adf_result[1]}")
print(f"Critical Values: {adf_result[4]}")
########################################

adf_test = ADFTest(alpha=0.05)
result = adf_test.should_diff(df)
print(f"ADF Test result: {result}")

train = df[:'2020']
test = df['2021':]

model = auto_arima(train, start_p=0, d=1, start_q=0,
                   max_p=5, max_d=5, max_q=5,
                   start_P=0, D=1, start_Q=0, max_P=5, max_D=5, max_Q=5,
                   m=12, seasonal=True, error_action='warn',
                   trace=True, suppress_warnings=True, stepwise=True,
                   random_state=20, n_fits=50)

print(model.summary())

predictions = pd.DataFrame(model.predict(n_periods=len(test)),
                           index=test.index)
predictions.columns = ['Predicted']


plt.figure(figsize=(10, 6))
plt.plot(train, label='Train', color='blue')
plt.plot(test, label='Test', color='orange')
plt.plot(predictions, label='Predicted', color='green')
plt.title("Train, Test, and Predicted Sales Data")
plt.xlabel('Year')
plt.ylabel('Sales')
plt.legend()
plt.show()


'''
โค้ดนี้ทำการวิเคราะห์ข้อมูลอนุกรมเวลา (yearly sales) โดยใช้โมเดล ARIMA เพื่อพยากรณ์ยอดขายในปีถัดไป 
โดยเริ่มจากการอ่านข้อมูล, ตรวจสอบสถานีของข้อมูล, แบ่งข้อมูลเป็นชุดการฝึกและทดสอบ, 
และสุดท้ายทำการพยากรณ์ยอดขายในปี 2021 และแสดงผลกราฟที่แสดงข้อมูลที่พยากรณ์ได้
'''