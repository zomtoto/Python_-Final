import os
import jaydebeapi
import pandas as pd

# 현재 스크립트 실행 디렉터리 기준 상대 경로 설정
base_dir = os.path.dirname(os.path.abspath(__file__))
h2_jar_path = os.path.join(base_dir, "jar/h2-2.3.232.jar")  # JAR 파일 상대 경로
csv_file_path = os.path.join(base_dir, "csv/상품목록.csv")  # CSV 파일 상대 경로

# H2 데이터베이스 연결 정보
db_url = "jdbc:h2:tcp://localhost/~/test"  # H2 데이터베이스 URL
username = "sa"
password = ""

try:
    # H2 연결 설정
    conn = jaydebeapi.connect(
        "org.h2.Driver",
        db_url,
        [username, password],
        h2_jar_path
    )
    cursor = conn.cursor()

    # CSV 파일 읽기
    try:
        df = pd.read_csv(csv_file_path, encoding='utf-8')
        print("CSV 파일 로드 성공!")
    except Exception as e:
        print(f"CSV 파일 로드 실패: {e}")
        conn.close()
        exit()

    # 열 이름 정리 (공백 제거 및 소문자 변환)
    df.columns = df.columns.str.strip().str.lower()

    # 카테고리 매핑 테이블 (문자열 -> 숫자 변환)
    category_mapping = {
        "art": 1,
        "cases": 2,
        "stationery": 3,
        "writing": 4
    }

    # 카테고리 문자열을 숫자로 변환 (없을 경우 기본값 -1로 설정)
    df['category_no'] = df['category_no'].map(category_mapping).fillna(-1).astype(int)

    # 빈 값 처리 및 데이터 정리
    df['in_price'] = df['in_price'].astype(str).str.replace(',', '').astype(int)
    df['out_price'] = df['out_price'].astype(str).str.replace(',', '').astype(int)
    df['sell_count'] = df['sell_count'].fillna(0).astype(int)  # 판매량 기본값 0
    df['quantity'] = df['quantity'].fillna(0).astype(int)  # 재고 기본값 0
    df['visit'] = df['visit'].fillna(0).astype(int)  # 조회수 기본값 0
    df['seal_service'] = df['seal_service'].replace({'T': 'True', 'F': 'False'}).fillna('False')  # 각인 여부 기본값 False

    # `delete` 컬럼 추가 (기본값 'False' 설정)
    if 'delete' not in df.columns:
        df['delete'] = 'False'

    # `product_no` 열이 존재하면 제거
    if 'product_no' in df.columns:
        df = df.drop(columns=['product_no'])

    # 데이터 삽입 SQL 템플릿
    insert_sql = """
    INSERT INTO product_table (category_no, name, company, in_price, out_price, sell_count, quantity, visit, seal_service, delete)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # 데이터프레임에서 각 행 삽입
    errors = []
    for _, row in df.iterrows():
        try:
            cursor.execute(insert_sql, [
                row['category_no'],
                row['name'],
                row['company'],
                row['in_price'],
                row['out_price'],
                row['sell_count'],
                row['quantity'],
                row['visit'],
                row['seal_service'],
                row['delete']
            ])
        except Exception as e:
            errors.append((row['name'], str(e)))

    # 커밋 및 연결 닫기
    conn.commit()
    cursor.close()
    conn.close()

    # 결과 출력
    if errors:
        print(f"{len(errors)}개의 데이터 삽입 중 오류 발생:")
        for error in errors:
            print(f"상품 이름: {error[0]}, 오류: {error[1]}")
    else:
        print("CSV 데이터가 성공적으로 H2 데이터베이스에 등록되었습니다!")

except Exception as e:
    print(f"데이터베이스 연결 실패 또는 처리 중 오류 발생: {e}")
