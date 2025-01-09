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
                              member_no INTEGER PRIMARY KEY AUTO_INCREMENT, -- 회원번호
                              id VARCHAR(50) NOT NULL UNIQUE,               -- 아이디
                              password VARCHAR(100) NOT NULL,              -- 비밀번호
                              name VARCHAR(100) NOT NULL,                  -- 이름
                              dob VARCHAR(10),                             -- 생년월일 (stored as string in YYYY-MM-DD format)
                              gender VARCHAR(10) CHECK(gender IN ('남', '여')), -- 성별
                              address VARCHAR(255),                        -- 주소
                              email VARCHAR(100),                          -- 이메일
                              phone VARCHAR(20),                           -- 전화번호
                              admin VARCHAR(10) CHECK (admin IN ('Y', 'N')), -- 관리자 여부
                              joinDate VARCHAR(10),                        -- 회원가입일 (stored as string in YYYY-MM-DD format)
                              delete VARCHAR(10) CHECK (delete IN ('True', 'False')) -- 삭제 여부
);

CREATE TABLE category_table (
                                category_no INTEGER PRIMARY KEY AUTO_INCREMENT, -- 카테고리번호
                                name VARCHAR(100) NOT NULL,                     -- 이름
                                delete VARCHAR(10) CHECK (delete IN ('True', 'False'))  -- 삭제 여부
);

CREATE TABLE product_table (
                               product_no INTEGER PRIMARY KEY AUTO_INCREMENT, -- 상품번호
                               category_no INTEGER,                           -- 카테고리번호 (FK)
                               name VARCHAR(100) NOT NULL,                   -- 상품이름
                               company VARCHAR(100),                         -- 회사명
                               in_price INTEGER NOT NULL,                    -- 입고가
                               out_price INTEGER NOT NULL,                   -- 판매가
                               sell_count INTEGER DEFAULT 0,                 -- 판매량
                               quantity INTEGER,                              -- 재고
                               visit INTEGER DEFAULT 0,                       -- 조회수
                               seal_service VARCHAR(10) CHECK (seal_service IN ('True', 'False')), -- 각인서비스
                               delete VARCHAR(10) CHECK (delete IN ('True', 'False')), -- 삭제 여부
                               FOREIGN KEY (category_no) REFERENCES category_table(category_no)
);

CREATE TABLE buy_table (
                           buy_no INTEGER PRIMARY KEY AUTO_INCREMENT,    -- 구매번호
                           member_no INTEGER,                            -- 회원번호 (FK)
                           product_no INTEGER,                           -- 상품번호 (FK)
                           date VARCHAR(10),                              -- 구매날짜 (stored as string in YYYY-MM-DD format)
                           quantity INTEGER,                              -- 구매수량
                           seal_service VARCHAR(10),                       -- 각인여부
                           total_price INTEGER,                           -- 총구매액
                           method VARCHAR(50),                            -- 구매방식
                           FOREIGN KEY (member_no) REFERENCES member_table(member_no),
                           FOREIGN KEY (product_no) REFERENCES product_table(product_no)
);

CREATE TABLE image_table (
                             image_no INTEGER PRIMARY KEY AUTO_INCREMENT,   -- 이미지번호
                             product_no INTEGER NULL,                       -- 상품번호 (FK, nullable)
                             origin_path VARCHAR(255),                      -- 원본이미지 경로
                             save_path VARCHAR(255),                        -- 저장 이미지 경로
                             image_name VARCHAR(255) NOT NULL,                    -- 이미지 이름
                             image_description TEXT,                              -- 이미지(분석) 설명
                             save_date DATE,                         -- 저장날짜 (stored as string in YYYY-MM-DD format)
                             update_date DATE,                       -- 수정날짜 (stored as string in YYYY-MM-DD format)
                             delete VARCHAR(10) CHECK (delete IN ('True', 'False')), -- 삭제 여부
                             FOREIGN KEY (product_no) REFERENCES product_table(product_no)
);

CREATE TABLE analyze_table (
                               analyze_no INTEGER PRIMARY KEY AUTO_INCREMENT,       -- 분석번호
                               image_no INTEGER NOT NULL,                           -- 이미지번호 (FK, image_table)
                               member_no INTEGER NOT NULL,                          -- 회원번호 (FK, member_table)
                               analyze_year INTEGER NOT NULL,                               -- 연도 // 이미지 관련 연도 기입
                               graph_type VARCHAR(255) NOT NULL,                        -- 그래프 종류
                               FOREIGN KEY (image_no) REFERENCES image_table(image_no), -- FK to image_table
                               FOREIGN KEY (member_no) REFERENCES member_table(member_no) -- FK to member_table
);

INSERT INTO category_table (name, delete) VALUES
                                              ('미술', 'False'),
                                              ('필통', 'False'),
                                              ('문구류', 'False'),
                                              ('필기류', 'False');
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
