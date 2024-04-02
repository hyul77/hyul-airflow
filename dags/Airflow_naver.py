from sqlalchemy import create_engine
import json
import pandas as pd
import requests
from tqdm import tqdm
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



from sqlalchemy import create_engine



# 기본 인수
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 3, 27),
}

# DAG 정의
dag = DAG(
    'naver_review_scraper',
    default_args=default_args,
    description='Scraping Naver reviews for restaurants',
    schedule_interval=timedelta(days=1),  # DAG가 매일 실행됩니다.
)


def scrape_naver_reviews_function():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import json
    import pandas as pd
    import requests
    from tqdm import tqdm
    import time

    # SQLAlchemy 엔진 생성
    engine = create_engine('mysql://hyul:ektmftkfkd231210@172.20.51.1/mydata')

    # MySQL 데이터베이스에서 데이터를 쿼리하여 DataFrame으로 읽기
    query = "SELECT * FROM restaurant_id"  # 테이블 이름에 맞게 수정
    # 연결 객체 생성
    conn = engine.connect()
    # 쿼리 실행 및 결과 반환
    df_from_mysql = pd.read_sql(query, con=conn)
    # 연결 종료
    conn.close()


    # 식당 id 리스트로 변경
    test_res = df_from_mysql['restaurant_id'].to_list()

    url = "https://api.place.naver.com/graphql"


    for i in tqdm(range(len(test_res)), desc="Processing"):
        all_items = []
        time.sleep(7)
        for j in range(3):
            try:
                payload = [{
                    "operationName": "getVisitorReviews",
                    "variables": { 
                        "input": {
                            "businessId": str(test_res[i]),
                            "businessType": "restaurant",
                            "item": "0",
                            "bookingBusinessId": "496515",
                            "page": j,
                            "size": 50,
                            "isPhotoUsed": False,
                            "includeContent": True,
                            "getUserStats": True,
                            "includeReceiptPhotos": True,
                            "cidList": ["220036", "220039", "220091", "1005863"],
                            "getReactions": True,
                            "getTrailer": True
                        },
                        "id": test_res[i]
                    },
                    "query": "query getVisitorReviews($input: VisitorReviewsInput) {\n  visitorReviews(input: $input) {\n    items {\n      id\n      rating\n      author {\n        id\n        nickname\n        from\n        imageUrl\n        borderImageUrl\n        objectId\n        url\n        review {\n          totalCount\n          imageCount\n          avgRating\n          __typename\n        }\n        theme {\n          totalCount\n          __typename\n        }\n        isFollowing\n        followerCount\n        followRequested\n        __typename\n      }\n      body\n      thumbnail\n      media {\n        type\n        thumbnail\n        thumbnailRatio\n        class\n        videoId\n        videoOriginSource\n        trailerUrl\n        __typename\n      }\n      tags\n      status\n      visitCount\n      viewCount\n      visited\n      created\n      reply {\n        editUrl\n        body\n        editedBy\n        created\n        date\n        replyTitle\n        isReported\n        isSuspended\n        __typename\n      }\n      originType\n      item {\n        name\n        code\n        options\n        __typename\n      }\n      language\n      highlightOffsets\n      apolloCacheId\n      translatedText\n      businessName\n      showBookingItemName\n      bookingItemName\n      votedKeywords {\n        code\n        iconUrl\n        iconCode\n        displayName\n        __typename\n      }\n      userIdno\n      loginIdno\n      receiptInfoUrl\n      reactionStat {\n        id\n        typeCount {\n          name\n          count\n          __typename\n        }\n        totalCount\n        __typename\n      }\n      hasViewerReacted {\n        id\n        reacted\n        __typename\n      }\n      nickname\n      showPaymentInfo\n      __typename\n    }\n    starDistribution {\n      score\n      count\n      __typename\n    }\n    hideProductSelectBox\n    total\n    showRecommendationSort\n    itemReviewStats {\n      score\n      count\n      itemId\n      starDistribution {\n        score\n        count\n        __typename\n      }\n      __typename\n    }\n    reactionTypes {\n      name\n      emojiUrl\n      label\n      __typename\n    }\n    __typename\n  }\n}"
                    }]
                headers = {
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "ko",
                    "Access-Control-Request-Headers": "content-type,x-wtm-graphql",
                    "Access-Control-Request-Method": "POST",
                    "Content-Length": "2590",
                    "Content-Type": "application/json",
                    "Cookie": "NNB=CGO5EAXH36KGG; ASID=dbf8f38a00000185c8bf268d00000043;",
                    "Origin": "https://m.place.naver.com",
                    "Referer": f"https://m.place.naver.com/restaurant/{test_res[i]}/review/visitor?a=rrv.more&m=2&ssc=Mstore.restaurant&p=3&lcsurl=https%3A%2F%2Fm.place.naver.com%2Frestaurant%2F%7Btest_res%5Bi%5D%7D%2Freview%2Fvisitor&px=456&py=11097&sx=456&sy=487&ua_brand_0=Not%20A(Brand&ua_version_0=99&ua_brand_1=Google%20Chrome&ua_version_1=121&ua_brand_2=Chromium&ua_version_2=121&ua_architecture=x86&ua_platform=Windows&ua_platformVersion=15.0.0&ua_uaFullVersion=121.0.6167.140&ua_full_brand_0=Not%20A(Brand&ua_full_version_0=99.0.0.0&ua_full_brand_1=Google%20Chrome&ua_full_version_1=121.0.6167.140&ua_full_brand_2=Chromium&ua_full_version_2=121.0.6167.140&u=https%3A%2F%2Fm.place.naver.com%2Frestaurant%2F{test_res[i]}%2Fvisitor&reviewSort=recent",
                    "Sec-Ch-Ua": "\"Not A(Brand\";v=\"99\", \"Google Chrome\";v=\"121\", \"Chromium\";v=\"121\"",
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": "\"Windows\"",
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-site",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                    "X-Wtm-Graphql": "eyJhcmciOiIxMTA5MDI0NzA5IiwidHlwZSI6InJlc3RhdXJhbnQiLCJzb3VyY2UiOiJwbGFjZSJ9"
                }
                response = requests.post(url, headers=headers, data=json.dumps(payload))
                a = response.json()
                if a[0]['data']['visitorReviews'] is not None:
                    # 어제의 날짜를 확인하기 위해 현재 날짜를 구합니다.
                    yesterday_date = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%m.%d')
                    yesterday_date = '.'.join(str(int(num)) for num in yesterday_date.split('.'))
                    # 현재 날짜를 '.'로 분리하여 월과 일로 나눕니다.
                    current_month, current_day = yesterday_date.split('.')
                    # 가져온 데이터에서 댓글들을 하나씩 확인합니다.
                    for item in a[0]['data']['visitorReviews']['items']:
                        # 각 댓글의 생성 날짜를 확인합니다.
                        comment_date = item['created']
                        # 댓글의 생성 날짜를 '.'로 분리하여 월과 일로 나눕니다.
                        try:
                            _, comment_month, comment_day, _ = comment_date.split('.')
                        except ValueError:
                            try:
                                comment_month, comment_day, _ = comment_date.split('.')
                            except ValueError:
                                print("Error occurred at:", comment_date)
                        # 댓글이 어제의 날짜에 작성된 것인지 확인합니다.
                        if comment_month == current_month and comment_day == current_day:
                            # 어제의 날짜에 작성된 댓글이면 all_items 리스트에 추가합니다.
                            all_items.append(item)
            except Exception as e:
                print(f"Exception occurred: {str(e)}")
                print("Restarting the loop with a 150-second delay...")
                time.sleep(150)
                continue  # 예외 발생 시 해당 반복을 다시 실행합니다.
        # 모든 items를 DataFrame으로 변환
        if all_items:
            df = pd.DataFrame(all_items)
            df = df[['id', 'rating', 'body', 'thumbnail', 'visited', 'created', 'businessName']]
            engine = create_engine('mysql://hyul:ektmftkfkd231210@172.20.51.1/mydata')
            # DataFrame을 MySQL 데이터베이스 테이블로 저장
            df.to_sql('review2', con=engine, if_exists='append', index=False)
            engine.dispose()
        else:
            print("해당 음식점에는 어제의 리뷰가 없습니다.")
        

# PythonOperator를 사용하여 코드 실행을 정의합니다.
scrape_naver_reviews = PythonOperator(
    task_id='scrape_naver_reviews',
    python_callable=scrape_naver_reviews_function,  # 여기에 실행할 함수를 넣으세요.
    dag=dag,
)

# 작업 간의 의존성을 정의합니다.
scrape_naver_reviews
