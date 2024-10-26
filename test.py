import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# สร้างตัวอย่าง DataFrame
data = {
    'num_loves': [1, 2, 3, 4, 5, 6],
    'prediction': [1.5, 2.5, 3.5, 4.0, 4.5, 5.5],
    'category': ['A', 'A', 'B', 'B', 'A', 'B']  # หมวดหมู่ที่จะแยกสี
}
selected_data_pd = pd.DataFrame(data)

# สร้างกราฟ lmplot โดยใช้พารามิเตอร์เพิ่มเติม
sns.lmplot(
    data=selected_data_pd,
    x='num_loves',
    y='prediction',
    hue='category',  # แบ่งตามหมวดหมู่ 'A' และ 'B'
    # markers={'A': 'o', 'B': 's'},  # รูปแบบของจุดข้อมูล
    palette='deep',  # ใช้ชุดสี
    aspect=1.5,
    height=6,  # ความสูงของกราฟ
)

plt.title('Linear Regression of Predictions vs Actual Loves')
plt.show()
