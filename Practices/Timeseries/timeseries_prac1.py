import pandas as pd
import matplotlib.pyplot as plt
from pmdarima.arima import auto_arima
from pmdarima.arima import ADFTest
from statsmodels.tsa.stattools import adfuller

df = pd.read_csv('./year_sales.csv')

df['Year'] = pd.to_datetime(df['Year'])
df.set_index('Year', inplace=True)

df = df.sort_index()

df.plot()
plt.title("Yearly Sales Data")
plt.show()

adf_result = adfuller(df['Sales']) 
print(f"ADF Statistic: {adf_result[0]}")
print(f"p-value: {adf_result[1]}")
print(f"Critical Values: {adf_result[4]}")

adf_test = ADFTest(alpha=0.05)
result = adf_test.should_diff(df)
print(f"ADF Test result: {result}")

# คำนวณขนาดของ train และ test set
train_size = int(len(df) * 0.8)  # 80% ของข้อมูล
train = df.iloc[:train_size]      # ตั้งค่า train set เป็น 80% แรก
test = df.iloc[train_size:]        # ตั้งค่า test set เป็น 20% หลังจาก train

# สร้างโมเดล ARIMA
model = auto_arima(train, start_p=0, d=1, start_q=0,
                   max_p=5, max_d=5, max_q=5,
                   start_P=0, D=1, start_Q=0, max_P=5, max_D=5, max_Q=5,
                   m=12, seasonal=True, error_action='warn',
                   trace=True, suppress_warnings=True, stepwise=True,
                   random_state=20, n_fits=50) #กำหนดจำนวนรอบการฝึกเป็น 50

print(model.summary())

predictions = pd.DataFrame(model.predict(n_periods=len(test)), index=test.index)
predictions.columns = ['Predicted']

print(predictions.isna().sum())  # ดูว่ามี NaN กี่ค่าใน 'Predicted'
print(predictions.head())  # แสดงค่า Predicted เพื่อดูว่าข้อมูลดูปกติหรือไม่

actual_mean = test['Sales'].mean()
predicted_mean = predictions['Predicted'].mean()
print(f"Actual Mean Sales: {actual_mean}")
print(f"Predicted Mean Sales: {predicted_mean}")
if abs(actual_mean - predicted_mean) < 0.1 * actual_mean:
    print("The predicted mean is close to the actual mean.")
else:
    print("The predicted mean significantly deviates from the actual mean.")

# แสดงกราฟผลลัพธ์
plt.figure(figsize=(10, 6))
plt.plot(train, label='Train', color='blue')
plt.plot(test, label='Test', color='orange')
plt.plot(predictions, label='Predicted', color='green')
plt.title("Train, Test, and Predicted Sales Data")
plt.xlabel('Year')
plt.ylabel('Sales')
plt.legend()
plt.show()
