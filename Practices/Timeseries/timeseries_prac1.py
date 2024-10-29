import pandas as pd
import matplotlib.pyplot as plt
from pmdarima.arima import auto_arima
from pmdarima.arima import ADFTest
from statsmodels.tsa.stattools import adfuller

# อ่านข้อมูลจากไฟล์ CSV
df = pd.read_csv('./year_sales.csv')

# แปลง Year เป็น datetime และตั้งเป็น index
df['Year'] = pd.to_datetime(df['Year'])
df.set_index('Year', inplace=True)

# เรียงลำดับ DataFrame ตาม index
df = df.sort_index()

# จัดการกับค่าที่ซ้ำซ้อนโดยใช้ค่าล่าสุดของปีนั้น
df = df.groupby(df.index.to_period("M")).last()  # ใช้ last() เพื่อเก็บค่า Sales ล่าสุดในแต่ละเดือน

# แปลง PeriodIndex กลับเป็น DatetimeIndex
df.index = df.index.to_timestamp()

# ตรวจสอบข้อมูลหลังการจัดการซ้ำซ้อน
print("Data after removing duplicates:")
print(df)

# แสดงกราฟข้อมูลหลังการจัดการ
df.plot()
plt.title("Yearly Sales Data")
plt.show()

# ตรวจสอบสถานะของการเป็น stationary
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
                   random_state=20, n_fits=50)  # กำหนดจำนวนรอบการฝึกเป็น 50

print(model.summary())

# พยากรณ์ยอดขายสำหรับข้อมูลทดสอบ
predictions = pd.DataFrame(model.predict(n_periods=len(test)), index=test.index, columns=['Predicted'])

print(predictions.isna().sum())  # ดูว่ามี NaN กี่ค่าใน 'Predicted'
print(predictions.head())  # แสดงค่า Predicted เพื่อดูว่าข้อมูลดูปกติหรือไม่

# คำนวณและแสดงผลการเปรียบเทียบค่าเฉลี่ย
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
plt.plot(train.index, train, label='Train', color='blue')
plt.plot(test.index, test, label='Test', color='orange')
plt.plot(predictions.index, predictions, label='Predicted', color='green')
plt.title("Train, Test, Predicted and Future Sales Data")
plt.xlabel('Year')
plt.ylabel('Sales')
plt.legend()
plt.show()

# #### save model #########
# import joblib
# # เซฟโมเดล
# joblib.dump(model, 'arima_model.pkl')
# # โหลดโมเดล
# loaded_model = joblib.load('arima_model.pkl')