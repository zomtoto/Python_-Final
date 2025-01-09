import os
import pandas as pd
from jaydebeapi import connect

# 경로 설정
base_dir = os.path.dirname(os.path.abspath(__file__))
csv_folder = os.path.join(base_dir, 'csv')
h2_jar_path = os.path.join(base_dir, "jar/h2-2.3.232.jar")

# H2 데이터베이스 연결 설정
url = "jdbc:h2:tcp://localhost/~/test"
user = "sa"
password = ""

try:
    conn = connect("org.h2.Driver", url, [user, password], h2_jar_path)
    cursor = conn.cursor()
except Exception as e:
    print(f"데이터베이스 연결 실패: {e}")
    exit()

# CSV 파일 경로 목록 설정
csv_files = [
    os.path.join(csv_folder, '구매이력_2019년.csv'),
    os.path.join(csv_folder, '구매이력_2020년.csv'),
    os.path.join(csv_folder, '구매이력_2021년.csv'),
    os.path.join(csv_folder, '구매이력_2022년.csv'),
    os.path.join(csv_folder, '구매이력_2023년.csv'),
]

# CSV 파일 처리
for file_path in csv_files:
    try:
        # CSV 파일 읽기
        df = pd.read_csv(file_path, encoding='utf-8', dtype=str)

        # 모든 열 이름의 공백 제거
        df.columns = df.columns.str.strip()

        # 열 이름 변경 및 데이터 정리
        df = df.rename(columns={
            '구매_ID': 'buy_no',
            '구매_날짜': 'date',
            '구매자_ID': 'member_no',
            '상품_ID': 'product_no',
            '구매_수량': 'quantity',
            '각인_서비스': 'seal_service',
            '결제_방식': 'method',
            '총_결제_금액': 'total_price'
        })

        # 데이터 타입 변환
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
        df['total_price'] = df['total_price'].str.replace(',', '', regex=True).fillna('0').astype(int)
        df['product_no'] = df['product_no'].str.extract(r'(\d+)').fillna(0).astype(int)

        # 데이터 삽입
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO buy_table (member_no, product_no, date, quantity, seal_service, total_price, method)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['member_no'],
                    row['product_no'],
                    row['date'],
                    row['quantity'],
                    row['seal_service'],
                    row['total_price'],
                    row['method']
                ))
            except Exception as e:
                print(f"데이터 삽입 오류: {e} | 데이터: {row.to_dict()}")
    except Exception as e:
        print(f"파일 처리 중 오류 발생: {file_path}, 오류 메시지: {e}")

# 커밋 및 연결 종료
try:
    conn.commit()
    print("구매 이력 데이터가 성공적으로 H2 데이터베이스에 등록되었습니다!")
finally:
    conn.close()
