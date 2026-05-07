from pyspark.sql.functions import udf, greatest, lit
from pyspark.sql.types import IntegerType, DoubleType, StringType


# ── Bảng breakpoint VN_AQI
# Đơn vị: µg/m³ (CO tính theo µg/m³, chú ý CO API trả về µg/m³)
# Format: (BP_i, BP_i+1, I_i, I_i+1)

BP_O3_1H = [
    (0,    160,   0,   50),
    (160,  200,  50,  100),
    (200,  300, 100,  150),
    (300,  400, 150,  200),
    (400,  800, 200,  300),
    (800, 1000, 300,  400),
    (1000, 1200, 400, 500),
]

BP_O3_8H = [
    (0,   100,   0,  50),
    (100, 120,  50, 100),
    (120, 170, 100, 150),
    (170, 210, 150, 200),
    (210, 400, 200, 300),
]

BP_CO = [
    # CO đơn vị µg/m³ (API trả về µg/m³)
    # 1 mg/m³ = 1000 µg/m³
    # Bảng gốc: 10000, 30000, 45000, 60000, 90000, 120000, 150000 µg/m³
    (0,      10000,   0,  50),
    (10000,  30000,  50, 100),
    (30000,  45000, 100, 150),
    (45000,  60000, 150, 200),
    (60000,  90000, 200, 300),
    (90000, 120000, 300, 400),
    (120000, 150000, 400, 500),
]

BP_SO2 = [
    (0,    125,   0,  50),
    (125,  350,  50, 100),
    (350,  550, 100, 150),
    (550,  800, 150, 200),
    (800, 1600, 200, 300),
    (1600, 2100, 300, 400),
    (2100, 2630, 400, 500),
]

BP_NO2 = [
    (0,    100,   0,  50),
    (100,  200,  50, 100),
    (200,  700, 100, 150),
    (700, 1200, 150, 200),
    (1200, 2350, 200, 300),
    (2350, 3100, 300, 400),
    (3100, 3850, 400, 500),
]

BP_PM10 = [
    (0,    50,   0,  50),
    (50,  150,  50, 100),
    (150, 250, 100, 150),
    (250, 350, 150, 200),
    (350, 420, 200, 300),
    (420, 500, 300, 400),
    (500, 600, 400, 500),
]

BP_PM25 = [
    (0,   25,   0,  50),
    (25,  50,  50, 100),
    (50,  80, 100, 150),
    (80, 150, 150, 200),
    (150, 250, 200, 300),
    (250, 350, 300, 400),
    (350, 500, 400, 500),
]


# Hàm tính AQI từng thông số (công thức nội suy tuyến tính)
def _calc_aqi_single(C, breakpoints):
    """
    Tính AQIx cho 1 thông số.
    C: nồng độ quan trắc (µg/m³)
    breakpoints: danh sách (BP_i, BP_i+1, I_i, I_i+1)
    """
    if C is None or C < 0:
        return 0
    for (bp_lo, bp_hi, i_lo, i_hi) in breakpoints:
        if bp_lo <= C <= bp_hi:
            return round(
                ((i_hi - i_lo) / (bp_hi - bp_lo)) * (C - bp_lo) + i_lo
            )
    # Vượt ngưỡng tối đa
    return 500


# Nowcast cho PM2.5 và PM10 
def calc_nowcast(hourly_values: list) -> float:
    """
    Tính giá trị Nowcast từ 12 giá trị trung bình 1 giờ gần nhất.
    hourly_values: [c1, c2, ..., c12] (c1 = giờ hiện tại, c12 = 12h trước)
    Theo QĐ-TCMT 2019:
      w* = Cmin/Cmax
      nếu w* <= 0.5 thì w = 0.5
      nếu w* > 0.5 thì w = w*
    """
    # Lọc None
    valid = [v for v in hourly_values if v is not None]

    # Cần ít nhất 2 trong 3 giá trị đầu (c1, c2, c3)
    first3_valid = sum(1 for v in hourly_values[:3] if v is not None)
    if first3_valid < 2:
        return None

    if len(valid) == 0:
        return None

    c_min = min(valid)
    c_max = max(valid)

    if c_max == 0:
        return 0.0

    w_star = c_min / c_max
    w = max(0.5, w_star)

    # Tính Nowcast theo công thức có trọng số
    numerator   = 0.0
    denominator = 0.0
    for i, c in enumerate(hourly_values):
        weight = w ** i
        if c is not None:
            numerator   += weight * c
            denominator += weight
        # Nếu ci = None thì w^(i-1) = 0, bỏ qua

    if denominator == 0:
        return None

    return round(numerator / denominator, 2)


# UDF đăng ký với Spark 

def _aqi_o3_1h(c):
    return _calc_aqi_single(c, BP_O3_1H) if c is not None else 0

def _aqi_o3_8h(c):
    return _calc_aqi_single(c, BP_O3_8H) if c is not None else 0

def _aqi_co(c):
    return _calc_aqi_single(c, BP_CO) if c is not None else 0

def _aqi_so2(c):
    return _calc_aqi_single(c, BP_SO2) if c is not None else 0

def _aqi_no2(c):
    return _calc_aqi_single(c, BP_NO2) if c is not None else 0

def _aqi_pm10(nowcast):
    return _calc_aqi_single(nowcast, BP_PM10) if nowcast is not None else 0

def _aqi_pm25(nowcast):
    return _calc_aqi_single(nowcast, BP_PM25) if nowcast is not None else 0

def _aqi_category(aqi):
    if aqi is None or aqi <= 0: return "Không xác định"
    if aqi <= 50:  return "Tốt"
    if aqi <= 100: return "Trung bình"
    if aqi <= 150: return "Kém"
    if aqi <= 200: return "Xấu"
    if aqi <= 300: return "Rất xấu"
    return "Nguy hại"

def _aqi_color(aqi):
    """Màu sắc theo Bảng 1 QĐ-TCMT 2019"""
    if aqi is None or aqi <= 0: return "#CCCCCC"
    if aqi <= 50:  return "#00E400"   # Xanh lá
    if aqi <= 100: return "#FFFF00"   # Vàng
    if aqi <= 150: return "#FF7E00"   # Da cam
    if aqi <= 200: return "#FF0000"   # Đỏ
    if aqi <= 300: return "#8F3F97"   # Tím
    return "#7E0023"                  # Nâu đỏ

def _health_advice(aqi):
    """Khuyến nghị sức khỏe theo Bảng 4+5 QĐ-TCMT 2019"""
    if aqi is None or aqi <= 0:
        return "Không có dữ liệu"
    if aqi <= 50:
        return "Chất lượng không khí tốt. Tự do hoạt động ngoài trời."
    if aqi <= 100:
        return "Chấp nhận được. Người nhạy cảm nên theo dõi triệu chứng."
    if aqi <= 150:
        return "Người nhạy cảm nên giảm hoạt động ngoài trời."
    if aqi <= 200:
        return "Mọi người nên giảm hoạt động ngoài trời, đeo khẩu trang."
    if aqi <= 300:
        return "Hạn chế tối đa hoạt động ngoài trời. Đeo khẩu trang khi ra ngoài."
    return "Ở trong nhà, đóng cửa sổ. Bắt buộc đeo khẩu trang nếu ra ngoài."


# Đăng ký UDF cho Spark
udf_aqi_o3_1h    = udf(_aqi_o3_1h,    IntegerType())
udf_aqi_o3_8h    = udf(_aqi_o3_8h,    IntegerType())
udf_aqi_co       = udf(_aqi_co,       IntegerType())
udf_aqi_so2      = udf(_aqi_so2,      IntegerType())
udf_aqi_no2      = udf(_aqi_no2,      IntegerType())
udf_aqi_pm10     = udf(_aqi_pm10,     IntegerType())
udf_aqi_pm25     = udf(_aqi_pm25,     IntegerType())
udf_aqi_category = udf(_aqi_category, StringType())
udf_aqi_color    = udf(_aqi_color,    StringType())
udf_health_advice= udf(_health_advice, StringType())