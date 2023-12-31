--arjun king
ALTER TABLE CUST_ACCOUNT_DTLS ADD CONSTRAINT UK_CUST_ACCOUNT_NO UNIQUE(ACCOUNT_NO) USING INDEX TABLESPACE PAYMENT_GATEWAY_INDEXES 

ALTER TABLE NOTIFICATION_DTLS MODIFY ACCOUNT_NO NOT NULL
ALTER TABLE NOTIFICATION_DTLS ADD CONSTRAINT FK_ACCOUNT_NO FOREIGN KEY(ACCOUNT_NO) REFERENCES CUST_ACCOUNT_DTLS(ACCOUNT_NO)

CREATE SEQUENCE SEQ_BANK_ID START WITH 301 INCREMENT BY 1 NOCACHE

CREATE SEQUENCE SEQ_TRAN_ID START WITH 1110001 INCREMENT BY 1 NOCACHE

create table ERROR_LOG
(
  module_name   VARCHAR2(30),
  error_no      NUMBER,
  error_date    DATE not null,
  error_msg     VARCHAR2(512),
  error_line_no VARCHAR2(30),
  status        VARCHAR2(10) default 'A'
)
tablespace PAYMENT_GATEWAY_TAB

CREATE TABLE EXT_MST_BANK_DTLS(BANK_NAME VARCHAR2(30),BANK_ADDRESS VARCHAR2(50),PHONE_NO_1 NUMBER,PHONE_NO_2 NUMBER,EMAIL_ID_1 VARCHAR2(30),EMAIL_ID_2 VARCHAR2(30),STATUS VARCHAR2(10))
ORGANIZATION EXTERNAL
(
TYPE ORACLE_LOADER
DEFAULT DIRECTORY DATA_PUMP_DIR
ACCESS PARAMETERS
(
RECORDS DELIMITED BY NEWLINE
SKIP 1
NOBADFILE NODISCARDFILE NOLOGFILE
FIELDS TERMINATED BY ','
MISSING FIELD VALUES ARE NULL
)
LOCATION('NEW_BANK_DTLS_05_12_2023.CSV')
)
REJECT LIMIT UNLIMITED

SELECT * FROM EXT_MST_BANK_DTLS

SELECT * FROM MST_ACCOUNT_TYPE
SELECT * FROM MST_TRAN_TYPE
SELECT * FROM CUST_ACCOUNT_DTLS
SELECT * FROM NOTIFICATION_DTLS 
SELECT * FROM MST_BANK_DTLS
SELECT * FROM CUST_PAYMENT_DTLS

--PROCEDURE TO INSERT DETAILS INTO NOTIFICATION_DTLS TABLE
CREATE OR REPLACE PROCEDURE SP_NOTIFICATIONS(P_ACCOUNT_NO NUMBER,P_AMOUNT NUMBER,P_TRAN_STATUS VARCHAR2,P_TO_BANK VARCHAR2,P_PHONE_NO NUMBER DEFAULT NULL,P_EMAIL VARCHAR2 DEFAULT NULL,P_REASON VARCHAR2)
AS
LN_CUST_PHNO NOTIFICATION_DTLS.CUST_PHNO%TYPE;
LV_CUST_EMAIL NOTIFICATION_DTLS.CUST_EMAIL%TYPE;
BEGIN
  IF P_PHONE_NO IS NULL THEN
    SELECT C.PHONE_NO INTO LN_CUST_PHNO FROM CUST_ACCOUNT_DTLS C WHERE C.ACCOUNT_NO=P_ACCOUNT_NO;
    ELSE 
      LN_CUST_PHNO:=P_PHONE_NO;
  END IF;
  IF P_EMAIL IS NULL THEN
    SELECT C.EMAIL INTO LV_CUST_EMAIL FROM CUST_ACCOUNT_DTLS C WHERE C.ACCOUNT_NO=P_ACCOUNT_NO;
    ELSE
      LV_CUST_EMAIL:=P_EMAIL;
   END IF;
  INSERT INTO  NOTIFICATION_DTLS(NOT_ID,
                                 NOT_DATE,
                                 ACCOUNT_NO,
                                 AMOUNT,
                                 TRAN_STATUS,
                                 TO_BANK,
                                 CUST_PHNO,
                                 CUST_EMAIL,
                                 REASON
                                 )
                          VALUES(SEQ_NOT_ID.NEXTVAL,
                                 SYSDATE,
                                 P_ACCOUNT_NO,
                                 P_AMOUNT,
                                 P_TRAN_STATUS,
                                 P_TO_BANK,
                                 LN_CUST_PHNO,
                                 LV_CUST_EMAIL,
                                 P_REASON
                                 );
  EXCEPTION
   WHEN OTHERS THEN
     SP_ERROR_LOG('SP_NOTIFICATIONS',SQLCODE,SQLERRM,REGEXP_REPLACE(SUBSTR(DBMS_UTILITY.format_error_backtrace,-10),'\D'));
     RAISE_APPLICATION_ERROR(-20300,SQLERRM);
END;
  
--PROCEDURE TO INSERT INTO MST_BANK_DTLS FROM EXTERNAL TABLE
CREATE OR REPLACE PROCEDURE SP_MST_BANK_DTLS
AS
LV_FILE_NAME VARCHAR2(100);
LV_FILE_STATUS BOOLEAN;
LN_FILE_SIZE BINARY_INTEGER;
LN_FIEL_SIZE_GB BINARY_INTEGER;
BEGIN
  LV_FILE_NAME:='NEW_BANK_DTLS_'||TO_CHAR(SYSDATE,'DD_MM_YYYY')||'.CSV';
  
  UTL_FILE.fgetattr('DATA_PUMP_DIR',LV_FILE_NAME,LV_FILE_STATUS,LN_FILE_SIZE,LN_FIEL_SIZE_GB);
  IF LV_FILE_STATUS THEN
   EXECUTE IMMEDIATE 'ALTER TABLE EXT_MST_BANK_DTLS LOCATION('||CHR(39)||LV_FILE_NAME||CHR(39)||')';
   MERGE INTO MST_BANK_DTLS T USING EXT_MST_BANK_DTLS S ON (T.BANK_NAME=S.BANK_NAME)
   WHEN MATCHED THEN
     UPDATE SET T.BANK_ADDRESS=S.BANK_ADDRESS,T.PHONE_NO_1=S.PHONE_NO_1,T.PHONE_NO_2=S.PHONE_NO_2,T.EMAIL_1=S.EMAIL_ID_1,T.EMAIL_2=S.EMAIL_ID_2,T.STATUS=S.STATUS
     WHEN NOT MATCHED THEN
       INSERT  VALUES(SEQ_BANK_ID.NEXTVAL,S.BANK_NAME,S.BANK_ADDRESS,S.PHONE_NO_1,S.PHONE_NO_2,S.EMAIL_ID_1,S.EMAIL_ID_2,S.STATUS);
  END IF;
  EXCEPTION 
    WHEN OTHERS THEN
      SP_ERROR_LOG(P_MODULE   => 'SP_MST_BANK_DTLS',
                   P_ERROR_NO => SQLCODE,
                   P_ERR_MSG  => SQLERRM,
                   P_LINE_NO  => REGEXP_REPLACE(SUBSTR(DBMS_UTILITY.format_error_backtrace,-10),'\D'));
       RAISE_APPLICATION_ERROR(-20455,SQLERRM);
END; 

--JOB ON ABOVE PROCEDURE
DECLARE 
X NUMBER;
BEGIN
  DBMS_JOB.submit(X,'BEGIN SP_MST_BANK_DTLS; END;',TRUNC(SYSDATE+1)+6/24,'SYSDATE+1');
  COMMIT;
END;

--FUNCTION TO GET TRANSACTION TYPE ID BY PASSING TYPE NAME
CREATE OR REPLACE FUNCTION SF_GET_TRAN_TYPE_ID(P_TRAN_NAME VARCHAR2)
RETURN NUMBER
AS
LN_TYPE_ID MST_TRAN_TYPE.TYPE_ID%TYPE;
BEGIN
  SELECT M.TYPE_ID INTO LN_TYPE_ID FROM MST_TRAN_TYPE M WHERE M.TYPE_NAME=P_TRAN_NAME;
  RETURN LN_TYPE_ID;
END;
--PROCEDURE TO INSERT DETAILS INTO CUST_PAYMENT_DTLS TABLE
CREATE OR REPLACE PROCEDURE SP_CUST_PAYMENT_DTLS(P_AMOUNT NUMBER,P_FROM_ACCOUNT NUMBER,P_TO_ACCOUNT NUMBER,P_TO_BANK VARCHAR2,P_IFSC VARCHAR2,P_TRAN_STATUS VARCHAR2,P_TRAN_TYPE VARCHAR2,P_TRAN_CHARGES NUMBER)
AS

BEGIN
  INSERT INTO CUST_PAYMENT_DTLS(tran_id, 
                                tran_amount, 
                                tran_date, 
                                tran_from_account, 
                                tran_to_account, 
                                tran_to_bank, 
                                ifsc_code, 
                                tran_status, 
                                tran_type_id, 
                                tran_charges 
                                )
                         VALUES(SEQ_TRAN_TYPE_ID.NEXTVAL,
                                P_AMOUNT,
                                SYSDATE,
                                P_FROM_ACCOUNT,
                                P_TO_ACCOUNT,
                                P_TO_BANK,
                                P_IFSC,
                                P_TRAN_STATUS,
                                SF_GET_TRAN_TYPE_ID(P_TRAN_TYPE),
                                P_TRAN_CHARGES);
                                
 EXCEPTION 
    WHEN OTHERS THEN
      SP_ERROR_LOG(P_MODULE   => 'SP_CUST_PAYMENT_DTLS',
                   P_ERROR_NO => SQLCODE,
                   P_ERR_MSG  => SQLERRM,
                   P_LINE_NO  => REGEXP_REPLACE(SUBSTR(DBMS_UTILITY.format_error_backtrace,-10),'\D'));
       RAISE_APPLICATION_ERROR(-20455,SQLERRM);
END; 
