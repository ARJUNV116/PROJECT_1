
CREATE TABLE CUST_PAYMENT_DTLS(TRAN_ID NUMBER CONSTRAINT PK_TRAN_ID PRIMARY KEY USING INDEX TABLESPACE PAYMENT_GATEWAY_INDEXES,TRAN_AMOUNT NUMBER(20,2),TRAN_DATE DATE,TRAN_FROM_ACCOUNT NUMBER,TRAN_TO_ACCOUNT NUMBER,TRAN_TO_BANK VARCHAR2(30),IFSC_CODE VARCHAR2(11),TRAN_STATUS VARCHAR2(10),TRAN_TYPE_ID NUMBER CONSTRAINT FK_TRAN_TYPE_ID REFERENCES MST_TRAN_TYPE(TYPE_ID),TRAN_CHARGES NUMBER(12,2),STATUS VARCHAR2(10))
TABLESPACE PAYMENT_GATEWAY_TAB
PARTITION BY LIST (TRAN_TO_BANK)
(
   PARTITION BANK_GROUP_1 VALUES ('PUNJAB NATIONAL BANK','INDIAN BANK','STATE BANK OF INDIA'),
   PARTITION BANK_GROUP_2 VALUES ('CANARA BANK','UNION BANK OF INDIA','INDIAN OVERSEAS BANK'),
   PARTITION BANK_GROUP_3 VALUES ('UCO BANK','BANK OF MAHARASHTRA','PUNJAB AND SINDH'),
   PARTITION BANK_GROUP_4 VALUES ('BANK OF INDIA','BANK OF BARODA','CENTRAL BANK OF INDIA'),
   PARTITION BANK_GROUP_DEFAULT VALUES (DEFAULT)
);

ALTER DATABASE DATAFILE 'C:\ORACLE_APP\ORADATA\ORCL\PAYMENT_GATEWAY_TAB.DBF' RESIZE 40M;

SELECT * FROM CUST_ACCOUNT_DTLS
--program to insert data into cust_payment_details
DECLARE 
LN_AMOUNT NUMBER;
LN_FAC NUMBER;
LN_TAC NUMBER;
LV_TBNAME VARCHAR2(40);
LV_IFSC VARCHAR2(12);
LV_TRAN_STATUS VARCHAR2(10);
LV_TRAN_TYPE VARCHAR2(30);
LN_TRAN_CHARGES NUMBER;
BEGIN
  FOR I IN 1..10 LOOP
  LN_AMOUNT:=TRUNC(DBMS_RANDOM.value(10000,100000));
  LN_FAC:=TRUNC(DBMS_RANDOM.VALUE(1111000011,1111000021));
  LN_TAC:=TRUNC(DBMS_RANDOM.VALUE(3333000011,7777000000));
  SELECT BANK_NAME INTO LV_TBNAME FROM(SELECT B.BANK_NAME  FROM MST_BANK_DTLS B ORDER BY DBMS_RANDOM.RANDOM)WHERE ROWNUM=1;
  LV_IFSC:=DBMS_RANDOM.STRING('U',5)||TRUNC(DBMS_RANDOM.value(111111,999999));
  SELECT SUBSTR('FPS',ROUND(DBMS_RANDOM.value(1,3)),1) INTO LV_TRAN_STATUS FROM DUAL;
   SELECT M.TYPE_NAME INTO LV_TRAN_TYPE FROM (SELECT * FROM MST_TRAN_TYPE ORDER BY DBMS_RANDOM.random) M WHERE ROWNUM=1;
 LN_TRAN_CHARGES:=ROUND(DBMS_RANDOM.value(2,500));
 SP_CUST_PAYMENT_DTLS(P_AMOUNT       => LN_AMOUNT,
                      P_FROM_ACCOUNT =>LN_FAC ,
                      P_TO_ACCOUNT   => LN_TAC,
                      P_TO_BANK      => LV_TBNAME,
                      P_IFSC         => LV_IFSC,
                      P_TRAN_STATUS  => LV_TRAN_STATUS,
                      P_TRAN_TYPE    =>LV_TRAN_TYPE ,
                      P_TRAN_CHARGES =>LN_TRAN_CHARGES );
   END LOOP;
 END;
 
CREATE TABLE SUCCESS_TRAN_DTLS TABLESPACE PAYMENT_GATEWAY_TAB AS SELECT * FROM CUST_PAYMENT_DTLS WHERE 1=2;
CREATE TABLE FAILURE_TRAN_DTLS TABLESPACE PAYMENT_GATEWAY_TAB AS SELECT * FROM CUST_PAYMENT_DTLS WHERE 1=2;
CREATE TABLE PENDING_TRAN_DTLS TABLESPACE PAYMENT_GATEWAY_TAB AS SELECT * FROM CUST_PAYMENT_DTLS WHERE 1=2

--PROCEDURE TO DIVIDE TRANSACTIONS AS F/S/P AND INSERT INTO SEPARATE TABLE
CREATE OR REPLACE PROCEDURE SP_SPLIT_TRANSACTIONS
AS
TYPE TYP_TRAN_DTLS IS TABLE OF CUST_PAYMENT_DTLS%ROWTYPE;
TYP TYP_TRAN_DTLS;
BEGIN
SELECT * BULK COLLECT INTO TYP FROM CUST_PAYMENT_DTLS T WHERE T.TRAN_STATUS='S' AND TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1);
 FORALL I IN 1..TYP.COUNT
  INSERT INTO SUCCESS_TRAN_DTLS VALUES (TYP(I).tran_id,
                                        TYP(I).tran_amount,
                                        TYP(I).tran_date,
                                        TYP(I).tran_from_account,
                                        TYP(I).tran_to_account,
                                        TYP(I).tran_to_bank,
                                        TYP(I).ifsc_code,
                                        TYP(I).tran_status,
                                        TYP(I).tran_type_id,
                                        TYP(I).tran_charges,
                                        TYP(I).status);

SELECT * BULK COLLECT INTO TYP FROM CUST_PAYMENT_DTLS T WHERE T.TRAN_STATUS='F' AND TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1);

FORALL I IN 1..TYP.COUNT
  INSERT INTO FAILURE_TRAN_DTLS VALUES (TYP(I).tran_id,
                                        TYP(I).tran_amount,
                                        TYP(I).tran_date,
                                        TYP(I).tran_from_account,
                                        TYP(I).tran_to_account,
                                        TYP(I).tran_to_bank,
                                        TYP(I).ifsc_code,
                                        TYP(I).tran_status,
                                        TYP(I).tran_type_id,
                                        TYP(I).tran_charges,
                                        TYP(I).status);
                                        
 SELECT * BULK COLLECT INTO TYP FROM CUST_PAYMENT_DTLS T WHERE T.TRAN_STATUS='P' AND TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1);

FORALL I IN 1..TYP.COUNT 
  INSERT INTO PENDING_TRAN_DTLS VALUES (TYP(I).tran_id,
                                        TYP(I).tran_amount,
                                        TYP(I).tran_date,
                                        TYP(I).tran_from_account,
                                        TYP(I).tran_to_account,
                                        TYP(I).tran_to_bank,
                                        TYP(I).ifsc_code,
                                        TYP(I).tran_status,
                                        TYP(I).tran_type_id,
                                        TYP(I).tran_charges,
                                        TYP(I).status);                               
  EXCEPTION
   WHEN OTHERS THEN
     SP_ERROR_LOG('program name',SQLCODE,SQLERRM,REGEXP_REPLACE(SUBSTR(DBMS_UTILITY.format_error_backtrace,-10),'\D'));
     RAISE_APPLICATION_ERROR(-20300,SQLERRM);
 END;
 
--JOB FOR SP_SPLIT_TRANSACTIONS
DECLARE 
X NUMBER;
BEGIN
  DBMS_JOB.submit(X,'BEGIN SP_SPLIT_TRANSACTIONS; END;',TRUNC(SYSDATE+1)+6/24,'SYSDATE+1');
  COMMIT;
END;

--procedure to daily total transaction amount report bank wise
CREATE OR REPLACE PROCEDURE SP_DAILY_BANK_AMOUNT_RPT
AS
CURSOR C1 IS SELECT T.TRAN_TO_BANK||','||SUM(T.TRAN_AMOUNT+T.TRAN_CHARGES) DATA FROM SUCCESS_TRAN_DTLS T WHERE TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1) GROUP BY T.TRAN_TO_BANK;
FO UTL_FILE.file_type;
LV_FILE_NAME VARCHAR2(100);
BEGIN
  LV_FILE_NAME:='DAILY_BANK_AMOUNT_RPT_'||TO_CHAR(SYSDATE,'DD_MM_YYYY')||'.CSV';
  FO:=UTL_FILE.fopen('DATA_PUMP_DIR',LV_FILE_NAME,'W',5000);
  UTL_FILE.putf(FO,'%s\n','BANK_NAME,TOTAL_TRAN_AMOUNT');
 FOR I IN C1 LOOP
   UTL_FILE.putf(FO,'%s\n',I.DATA);
 END LOOP;
 UTL_FILE.fclose(FO);
 EXCEPTION
   WHEN OTHERS THEN
     SP_ERROR_LOG('program name',SQLCODE,SQLERRM,REGEXP_REPLACE(SUBSTR(DBMS_UTILITY.format_error_backtrace,-10),'\D'));
     RAISE_APPLICATION_ERROR(-20300,SQLERRM);
END;

--JOB ON SP_DAILY_BANK_AMOUNT_RPT
DECLARE
X NUMBER;
BEGIN
  DBMS_JOB.submit(X,'BEGIN SP_DAILY_BANK_AMOUNT_RPT; END;',TRUNC(SYSDATE+1)+6/24+30/24/60,'SYSDATE+1');
  COMMIT;
END;

SELECT * FROM USER_JOBS
--TO MAKE JOB ON SP_DAILY_BANK_AMOUNT_RPT IN INACTIVE MODE
BEGIN
  DBMS_JOB.broken(103,TRUE);
  COMMIT;
END;


--PROCEDURE TO GENERATE BANK WISE SUCCESS,PENDING,FAILURE EXCEL SHEET REPORTS
CREATE OR REPLACE PROCEDURE SP_BANK_WISE_TRAN_DTLS_RPT
AS
TYPE TYP_BANK_NAME IS TABLE OF CUST_PAYMENT_DTLS.TRAN_TO_BANK%TYPE;
TYP TYP_BANK_NAME;
FO UTL_FILE.file_type;
LV_FILE_NAME VARCHAR2(100);
BEGIN
  --SUCCESS_TRAN_DTLS BANK WISE EXCELS
  SELECT UNIQUE T.TRAN_TO_BANK BULK COLLECT INTO TYP FROM SUCCESS_TRAN_DTLS T WHERE TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1);
  FOR I IN 1..TYP.COUNT LOOP
    LV_FILE_NAME:=TYP(I)||'_SUCCESS_TRAN_DTLS_'||TO_CHAR(SYSDATE,'DD_MM_YYYY')||'.CSV';
    FO:=UTL_FILE.fopen('DATA_PUMP_DIR',LV_FILE_NAME,'W',5000);
    UTL_FILE.putf(FO,'%s\n','tran_id,tran_amount,tran_date,tran_from_account,tran_to_account,tran_to_bank,ifsc_code,tran_status,tran_type_id,tran_charges,status');
    FOR J IN (SELECT * FROM SUCCESS_TRAN_DTLS T WHERE T.TRAN_TO_BANK=TYP(I) AND  TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1)) LOOP
      UTL_FILE.putf(FO,'%s\n',J.tran_id||','||J.tran_amount||','||J.tran_date||','||J.tran_from_account||','||J.tran_to_account||','||J.tran_to_bank||','||J.ifsc_code||','||J.tran_status||','||J.tran_type_id||','||J.tran_charges||','||J.status);
      END LOOP;
      UTL_FILE.fclose(FO);
   END LOOP;
 --failure_tran_dtls BANK WISE EXCELS
   SELECT UNIQUE T.TRAN_TO_BANK BULK COLLECT INTO TYP FROM failure_tran_dtls T WHERE TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1);
  FOR I IN 1..TYP.COUNT LOOP
    LV_FILE_NAME:=TYP(I)||'_FAILURE_TRAN_DTLS_'||TO_CHAR(SYSDATE,'DD_MM_YYYY')||'.CSV';
    FO:=UTL_FILE.fopen('DATA_PUMP_DIR',LV_FILE_NAME,'W',5000);
    UTL_FILE.putf(FO,'%s\n','tran_id,tran_amount,tran_date,tran_from_account,tran_to_account,tran_to_bank,ifsc_code,tran_status,tran_type_id,tran_charges,status');
    FOR J IN (SELECT * FROM failure_tran_dtls T WHERE T.TRAN_TO_BANK=TYP(I) AND  TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1)) LOOP
      UTL_FILE.putf(FO,'%s\n',J.tran_id||','||J.tran_amount||','||J.tran_date||','||J.tran_from_account||','||J.tran_to_account||','||J.tran_to_bank||','||J.ifsc_code||','||J.tran_status||','||J.tran_type_id||','||J.tran_charges||','||J.status);
      END LOOP;
      UTL_FILE.fclose(FO);
   END LOOP;
   --PENDING_TRAN_DTLS BANK WISE EXCELS
     SELECT UNIQUE T.TRAN_TO_BANK BULK COLLECT INTO TYP FROM pending_tran_dtls T WHERE TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1);
  FOR I IN 1..TYP.COUNT LOOP
    LV_FILE_NAME:=TYP(I)||'_PENDING_TRAN_DTLS_'||TO_CHAR(SYSDATE,'DD_MM_YYYY')||'.CSV';
    FO:=UTL_FILE.fopen('DATA_PUMP_DIR',LV_FILE_NAME,'W',5000);
    UTL_FILE.putf(FO,'%s\n','tran_id,tran_amount,tran_date,tran_from_account,tran_to_account,tran_to_bank,ifsc_code,tran_status,tran_type_id,tran_charges,status');
    FOR J IN (SELECT * FROM pending_tran_dtls T WHERE T.TRAN_TO_BANK=TYP(I) AND  TRUNC(T.TRAN_DATE)=TRUNC(SYSDATE-1)) LOOP
      UTL_FILE.putf(FO,'%s\n',J.tran_id||','||J.tran_amount||','||J.tran_date||','||J.tran_from_account||','||J.tran_to_account||','||J.tran_to_bank||','||J.ifsc_code||','||J.tran_status||','||J.tran_type_id||','||J.tran_charges||','||J.status);
      END LOOP;
      UTL_FILE.fclose(FO);
   END LOOP;
END;

--JOB ON SP_BANK_WISE_TRAN_DTLS_RPT
DECLARE
X NUMBER;
BEGIN
  DBMS_JOB.submit(X,'BEGIN SP_BANK_WISE_TRAN_DTLS_RPT; END;',TRUNC(SYSDATE+1)+7/24,'SYSDATE+1');
  COMMIT;
END;

SELECT * FROM USER_JOBS
--TO MAKE JOB ON SP_BANK_WISE_TRAN_DTLS_RPT IN INACTIVE MODE
BEGIN
  DBMS_JOB.broken(104,TRUE);
  COMMIT;
END;
