# LogCollector
고객 서버내 로그 파일 Local/Remote로 이동하기 위한 파일 핸들링 모듈

### 개발기간
    2021.01~2020.03(3개월)   
    
### 개발인원
    1인 개발

### 내용
    고객서버 파일 우리측 Local/Server/HDFS로 이동할 수 있는 파일 핸들링 모듈
    
### 개발배경   
    현재 우리의 환경에 맞추어 고객 웹 데이터 분석을 위한 데이터 수집 모듈 필요에 의한 개발
   
### 목적   
    - 현재 우리의 서버 및 노드 환경에 맞추어 고객 로그 파일을 수집하기 위함
    - Accesslog, DBlog, Aphachelog 등 필요 로그 데이터 유동적으로 수집 
   
### 개발 중 문제점 및 해결   
    [문제] 복수 개의 서버에서도 수집이 되어야함
    [해결] Config내 서버 정보를 기입하고 해당 정보를 토대로 Server 객체를 생성하여 원격 여러대의 서버를 고려하여 순회하며 수집할 수 있도록 변경
    
    [문제] HDFS로 직접 업로드시 다이렉트로 SFTP 세션을 열 수 없음
    [해결] Master Node내 Shell Script 작성 후 Shell Script를 커맨드 제어로 실행하여 Node-to-HDFS 형태로 이동하는 로직으로 변경
    
    [문제] 각기 서로 다른 디렉토리에 저장되어있는 파일도 핸들링 가능해야함
    [해결] Config 파일에 수집해야하는 파일과 디렉토리 기입시 모든 파일을 순회하며 수집하도록 로직 추가
    
    [문제] 파일 이동의 신뢰성이 보장되어야함
    [해결] 파일 이동 전/후의 파일 bytes를 체크하여 이동 신뢰성을 체크하고, 설정된 시도만큼 파일 이동을 재시도함
    
    [문제] 윈도우 환경에서 일정 주기로 돌아야함
    [해결] pyinstaller 활용 .exe 파일 생성 후 작업스케줄러 활용 Daily 배치화 함
    
    [문제] 수집 결과가 리포트 되어야함
    [해결] logger 활용 수집 결과 제공
    
    [문제] 각 메소드의 기능이 2개 이상 포함
    [해결] 여러번 리팩토링하여 각 메소드는 1개의 기능만을 수행하도록 변경
    
    [문제] 각 메소드 설명 부족
    [해결] 메소드마다 인자/return에 대한 데이터 타입 annotation 추가
        
### 결과
    약 6개월 이상 활용 중이나 고객측 서버 점검 및 VPN 세션 접속 종료 문제 외 수집 로직 특이사항 없음
    
### 동작방식
![image](https://user-images.githubusercontent.com/69191799/128655408-1728fe0f-9699-4e80-a311-c3388da72b58.png)

### 활용 라이브러리
    - paramiko
    - pyinstaller
