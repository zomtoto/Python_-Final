import os
import jpype
import jaydebeapi

# 현재 실행 디렉터리 설정
base_dir = os.path.dirname(os.path.abspath(__file__))
h2_jar_path = os.path.join(base_dir, "jar/h2-2.3.232.jar")

# H2 데이터베이스 연결 정보
db_url = "jdbc:h2:tcp://localhost/~/test"
username = "sa"
password = ""

# 테이블 생성 SQL
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

try:
    # H2 연결 설정
    conn = jaydebeapi.connect(
        "org.h2.Driver",
        db_url,
        [username, password],
        h2_jar_path
    )
    cursor = conn.cursor()
    print("H2 데이터베이스 연결 성공!")

    # 기존 데이터베이스 초기화
    cursor.execute("DROP ALL OBJECTS")
    print("기존 데이터베이스 초기화 완료!")

    # 테이블 생성 및 데이터 삽입
    for statement in create_sql.strip().split(";"):
        if statement.strip():
            cursor.execute(statement)
    print("테이블 생성 및 데이터 삽입 완료!")

    # 커밋 및 연결 닫기
    conn.commit()
    cursor.close()
    conn.close()

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
