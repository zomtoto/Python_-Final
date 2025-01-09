import os
import sys
import jpype
import jaydebeapi
import pandas as pd

# 리소스 경로 처리 함수 (PyInstaller EXE에서 리소스 접근용)
def resource_path(relative_path):
    try:
        # PyInstaller 실행 파일의 임시 디렉토리 경로
        base_path = sys._MEIPASS
    except AttributeError:
        # 개발 환경에서 실행할 때는 현재 경로 사용
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# 경로 설정
base_dir = resource_path(".")
csv_folder = resource_path("csv")
h2_jar_path = resource_path("jar/h2-2.3.232.jar")

# H2 데이터베이스 연결 정보
db_url = "jdbc:h2:tcp://localhost/~/test"
username = "sa"
password = ""


def initialize_database(cursor):
    # 데이터베이스 초기화 및 테이블 생성
    create_sql = """
    CREATE TABLE member_table (
        member_no INTEGER PRIMARY KEY AUTO_INCREMENT,
        id VARCHAR(50) NOT NULL UNIQUE,
        password VARCHAR(100) NOT NULL,
        name VARCHAR(100) NOT NULL,
        dob VARCHAR(10),
        gender VARCHAR(10) CHECK(gender IN ('남', '여')),
        address VARCHAR(255),
        email VARCHAR(100),
        phone VARCHAR(20),
        admin VARCHAR(10) CHECK (admin IN ('Y', 'N')),
        joinDate VARCHAR(10),
        delete VARCHAR(10) CHECK (delete IN ('True', 'False'))
    );

    CREATE TABLE category_table (
        category_no INTEGER PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100) NOT NULL,
        delete VARCHAR(10) CHECK (delete IN ('True', 'False'))
    );

    CREATE TABLE product_table (
        product_no INTEGER PRIMARY KEY AUTO_INCREMENT,
        category_no INTEGER,
        name VARCHAR(100) NOT NULL,
        company VARCHAR(100),
        in_price INTEGER NOT NULL,
        out_price INTEGER NOT NULL,
        sell_count INTEGER DEFAULT 0,
        quantity INTEGER,
        visit INTEGER DEFAULT 0,
        seal_service VARCHAR(10) CHECK (seal_service IN ('True', 'False')),
        delete VARCHAR(10) CHECK (delete IN ('True', 'False')),
        FOREIGN KEY (category_no) REFERENCES category_table(category_no)
    );

    CREATE TABLE buy_table (
        buy_no INTEGER PRIMARY KEY AUTO_INCREMENT,
        member_no INTEGER,
        product_no INTEGER,
        date VARCHAR(10),
        quantity INTEGER,
        seal_service VARCHAR(10),
        total_price INTEGER,
        method VARCHAR(50),
        FOREIGN KEY (member_no) REFERENCES member_table(member_no),
        FOREIGN KEY (product_no) REFERENCES product_table(product_no)
    );

    CREATE TABLE image_table (
        image_no INTEGER PRIMARY KEY AUTO_INCREMENT,
        product_no INTEGER,
        origin_path VARCHAR(255),
        save_path VARCHAR(255),
        save_date VARCHAR(10),
        update_date VARCHAR(10),
        delete VARCHAR(10) CHECK (delete IN ('True', 'False')),
        FOREIGN KEY (product_no) REFERENCES product_table(product_no)
    );

    INSERT INTO category_table (category_no, name, delete) VALUES
        (1, '미술', 'False'),
        (2, '필통', 'False'),
        (3, '문구류', 'False'),
        (4, '필기류', 'False');
    """

    print("기존 데이터베이스 초기화 중...")
    cursor.execute("DROP ALL OBJECTS")
    for statement in create_sql.strip().split(";"):
        if statement.strip():
            cursor.execute(statement)
    print("데이터베이스 초기화 및 테이블 생성 완료!")

def load_products(cursor):
    # 상품 목록 데이터를 H2 데이터베이스에 삽입

    csv_file_path = os.path.join(csv_folder, "상품목록.csv")
    print(f"상품 목록 파일 로드 중: {csv_file_path}")
    try:
        df = pd.read_csv(csv_file_path, encoding="utf-8")
        # 상품 데이터 처리 및 DB 삽입 로직 작성

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

        print("상품 목록 데이터 처리 완료!")
    except Exception as e:
        print(f"상품 목록 처리 중 오류 발생: {e}")

def load_members(cursor):
    # 회원 데이터를 H2 데이터베이스에 삽입

    csv_files = [os.path.join(csv_folder, f"회원목록_{year}년.csv") for year in range(2019, 2024)]
    for file_path in csv_files:
        print(f"회원 목록 파일 로드 중: {file_path}")
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
            # 회원 데이터 처리 및 DB 삽입 로직 작성

            # 필요한 열이 없을 경우 기본 값으로 설정
            required_columns = ['PID', '아이디', '비밀번호', '성함', '주민번호', '주소', '메일 주소', '회원_가입일', '전화번호']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = None

            # 데이터 정리
            df = df.rename(columns={
                '아이디': 'id',
                '비밀번호': 'password',
                '성함': 'name',
                '주민번호': 'dob_gender',
                '주소': 'address',
                '메일 주소': 'email',
                '회원_가입일': 'joinDate',
                '전화번호': 'phone'
            })

            # 생년월일과 성별 추출
            df['dob'] = df['dob_gender'].str[:6].apply(
                lambda x: f"19{x[:2]}-{x[2:4]}-{x[4:6]}" if len(x) >= 6 else None)
            df['gender'] = df['dob_gender'].str[-1].apply(
                lambda x: '남' if x in ['1', '3'] else ('여' if x in ['2', '4'] else None))

            # 불필요한 열 제거
            df = df[['id', 'password', 'name', 'dob', 'gender', 'address', 'email', 'phone', 'joinDate']]

            # 데이터 삽입
            for _, row in df.iterrows():
                cursor.execute("SELECT COUNT(*) FROM member_table WHERE id = ?", (row['id'],))
                if cursor.fetchone()[0] == 0:  # 중복 확인
                    cursor.execute("""
                                INSERT INTO member_table (id, password, name, dob, gender, address, email, phone, admin, joinDate, delete)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'N', ?, 'False')
                            """, (
                        row['id'], row['password'], row['name'], row['dob'],
                        row['gender'], row['address'], row['email'], row['phone'], row['joinDate']
                    ))
            # 커밋 및 연결 종료
            conn.commit()
            print(f"회원 목록 데이터 처리 완료: {file_path}")

        except Exception as e:
            print(f"회원 목록 처리 중 오류 발생: {e}")

def load_purchases(cursor):
    # 구매 데이터를 H2 데이터베이스에 삽입

    csv_files = [os.path.join(csv_folder, f"구매이력_{year}년.csv") for year in range(2019, 2024)]
    for file_path in csv_files:
        print(f"구매 이력 파일 로드 중: {file_path}")
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
            # 구매 데이터 처리 및 DB 삽입 로직 작성

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
            conn.commit()

        except Exception as e:
            print(f"파일 처리 중 오류 발생: {file_path}, 오류 메시지: {e}")

if __name__ == "__main__":
    try:
        # JVM 시작
        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=[h2_jar_path])

        # H2 데이터베이스 연결
        conn = jaydebeapi.connect("org.h2.Driver", db_url, [username, password], h2_jar_path)
        cursor = conn.cursor()

        # 작업 호출 (순서에 따라)
        initialize_database(cursor)
        load_products(cursor)
        load_members(cursor)
        load_purchases(cursor)

        # 작업 완료 후 커밋
        conn.commit()
        print("모든 작업이 완료되었습니다!")

    except Exception as e:
        print(f"오류 발생: {e}")

    finally:
        # 리소스 정리
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
        # 종료 전 대기
        input("프로그램이 종료되었습니다. Enter 키를 눌러 창을 닫으세요.")
