import calendar
from datetime import datetime

# Lấy ngày hiện tại
ngay_hien_tai = datetime.now()

# Lấy số ngày trong tháng hiện tại
so_ngay_trong_thang = calendar.monthrange(ngay_hien_tai.year, ngay_hien_tai.month)[1]

print(f"Ngày {ngay_hien_tai.day} Tháng {ngay_hien_tai.month} năm {ngay_hien_tai.year} có {so_ngay_trong_thang} ngày.")
