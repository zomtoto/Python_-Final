import os
import pandas as pd
from jaydebeapi import connect

# 현재 스크립트 실행 디렉터리 기준 상대 경로 설정
base_dir = os.path.dirname(os.path.abspath(__file__))
h2_jar_path = os.path.join(base_dir, "jar/h2-2.3.232.jar")  # JAR 파일 상대 경로
csv_folder_path = os.path.join(base_dir, "csv")  # CSV 폴더 상대 경로

# H2 데이터베이스 연결 설정
url = "jdbc:h2:tcp://localhost/~/test"
user = "sa"
password = ""
conn = connect("org.h2.Driver", url, [user, password], h2_jar_path)

# 기존 데이터 삭제
cursor = conn.cursor()
cursor.execute("DELETE FROM member_table")
print("기존 데이터 삭제 완료.")

# CSV 파일 목록
csv_files = [
    os.path.join(csv_folder_path, "회원목록_2019년.csv"),
    os.path.join(csv_folder_path, "회원목록_2020년.csv"),
    os.path.join(csv_folder_path, "회원목록_2021년.csv"),
    os.path.join(csv_folder_path, "회원목록_2022년.csv"),
    os.path.join(csv_folder_path, "회원목록_2023년.csv"),
]

# CSV 파일 처리
for file_path in csv_files:
    try:
        print(f"{file_path} 로드 성공!")
        df = pd.read_csv(file_path, encoding='utf-8')

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
        df['dob'] = df['dob_gender'].str[:6].apply(lambda x: f"19{x[:2]}-{x[2:4]}-{x[4:6]}" if len(x) >= 6 else None)
        df['gender'] = df['dob_gender'].str[-1].apply(lambda x: '남' if x in ['1', '3'] else ('여' if x in ['2', '4'] else None))

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

    except Exception as e:
        print(f"{file_path} 처리 중 오류 발생: {e}")

print("모든 CSV 데이터가 성공적으로 H2 데이터베이스에 등록되었습니다!")

# 커밋 및 연결 종료
conn.commit()
cursor.close()
conn.close()
