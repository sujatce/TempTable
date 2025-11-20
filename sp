CREATE OR ALTER PROCEDURE [dbo].[SP_Load_TEMP_NEW_TO_THERAPY]
AS
BEGIN
    SET NOCOUNT ON;

    ----------------------------------------------------------
    -- 1. Load full NTT table sorted like your Alteryx workflow
    ----------------------------------------------------------
    ;WITH NTT_Sorted AS
    (
        SELECT *
        FROM [ORCHESTRATION].[dbo].[NEW_TO_THERAPY]
        ORDER BY LOAD_DATETIME, RECORD_ID
    ),

    ----------------------------------------------------------
    -- 2. Dedup Active DIM_MEMBER on MEMBER_MBI (like your Select Distinct)
    ----------------------------------------------------------
    DIM AS
    (
        SELECT DISTINCT GMPI_ID, MEMBER_MBI
        FROM [BI_Analysis].[dbo].[DIM_MEMBER]
        WHERE MEMBER_MBI IS NOT NULL
          AND ACTIVE_FLAG = 1
    ),

    ----------------------------------------------------------
    -- 3. Join NTT to DIM_MEMBER on MBI → MEMBER_MBI
    --    Add PATIENT_MISMATCH = 1 when GMPI_ID is NULL
    ----------------------------------------------------------
    NTT_DIM AS
    (
        SELECT  
            n.*,
            d.GMPI_ID,
            CASE WHEN d.GMPI_ID IS NULL THEN 1 ELSE 0 END AS PATIENT_MISMATCH
        FROM NTT_Sorted n
        LEFT JOIN DIM d
            ON n.MBI = d.MEMBER_MBI
    ),

    ----------------------------------------------------------
    -- 4. Get latest active MRM rows
    ----------------------------------------------------------
    LatestMRM AS
    (
        SELECT mrm.GMPI_ID
        FROM [BI_Analysis].[dbo].[MEMBER_REPORT_MONTH] mrm
        WHERE mrm.DATA_ENTITY = 'WellMed'
          AND mrm.ACTIVE_FLAG = 1
          AND mrm.REPORT_MONTH = (
                SELECT MAX(rm.REPORT_MONTH)
                FROM [BI_Analysis].[dbo].[MEMBER_REPORT_MONTH] rm
                WHERE rm.DATA_ENTITY = 'WellMed'
          )
    ),

    ----------------------------------------------------------
    -- 5. Join NTT_DIM with MRM on GMPI_ID
    --    (LEFT JOIN because you UNION L & J outputs)
    ----------------------------------------------------------
    FINAL_DATA AS
    (
        SELECT 
            ntt.*,
            CASE WHEN mrm.GMPI_ID IS NULL THEN 1 ELSE 0 END AS MRM_MISSING
        FROM NTT_DIM ntt
        LEFT JOIN LatestMRM mrm
            ON ntt.GMPI_ID = mrm.GMPI_ID
    )

    ----------------------------------------------------------
    -- 6. INSERT INTO TEMP_NEW_TO_THERAPY (NO IDENTITY COLUMN)
    ----------------------------------------------------------
    INSERT INTO [dbo].[TEMP_NEW_TO_THERAPY]
    (
        GMPI_ID,
        PATIENT_MISMATCH,
        RPT_MO_KEY,
        REPORTDATE,
        HCONTRACT,
        CDO,
        CDO_SUBGROUP,
        YEAR_DOS,
        MONTH_DOS,
        BYY,
        MGMT_TYPE,
        PBP,
        PCP_NPI,
        ENTY_ID,
        PCP_TIN,
        EMP_STATUS,
        RISK_TYPE,
        MEASURE_YEAR,
        CONTRACT_PBP,
        SUPER_CDO,
        SUPER_CDO_SUBGROUP,
        MAD_FILLS_2Y,
        MAD_LATEST_FILL_DATE,
        MAD_DAYS_SINCE_LAST_FILL,
        MAD_NEW_TO_THERAPY,
        MAD_FILLS_180D,
        MAD_NTT_RESTART,
        MAD_LATEST_FILL_DAYS_SUPPLY,
        MAD_LATEST_FILL_QUANTITY,
        MAC_FILLS_2Y,
        MAC_LATEST_FILL_DATE,
        MAC_DAYS_SINCE_LAST_FILL,
        MAC_NEW_TO_THERAPY,
        MAC_FILLS_180D,
        MAC_NTT_RESTART,
        MAC_LATEST_FILL_DAYS_SUPPLY,
        MAC_LATEST_FILL_QUANTITY,
        MAH_FILLS_2Y,
        MAH_LATEST_FILL_DATE,
        MAH_DAYS_SINCE_LAST_FILL,
        MAH_NEW_TO_THERAPY,
        MAH_FILLS_180D,
        MAH_NTT_RESTART,
        MAH_LATEST_FILL_DAYS_SUPPLY,
        MAH_LATEST_FILL_QUANTITY,
        MBI,
        ENCRYPT,
        REPORT_DATE,
        OC_FLAG,
        EMR_FLAG,
        HC_OC_COMANAGED_FLAG,
        ENTERPRISE_INDV_ID,
        CLAIMS_REFRESH_DATE,
        MAC_NTT_GENERIC,
        MAD_NTT_GENERIC,
        MAH_NTT_GENERIC,
        MAC_NTT_MED_NAME,
        MAD_NTT_MED_NAME,
        MAH_NTT_MED_NAME,
        PROCESS_LOG_ID,
        LOAD_DATETIME,
        RECORD_ID,
        USER_STORY
    )
    SELECT
        GMPI_ID,
        PATIENT_MISMATCH,
        RPT_MO_KEY,
        REPORTDATE,
        HCONTRACT,
        CDO,
        CDO_SUBGROUP,
        YEAR_DOS,
        MONTH_DOS,
        BYY,
        MGMT_TYPE,
        PBP,
        PCP_NPI,
        ENTY_ID,
        PCP_TIN,
        EMP_STATUS,
        RISK_TYPE,
        MEASURE_YEAR,
        CONTRACT_PBP,
        SUPER_CDO,
        SUPER_CDO_SUBGROUP,
        MAD_FILLS_2Y,
        MAD_LATEST_FILL_DATE,
        MAD_DAYS_SINCE_LAST_FILL,
        MAD_NEW_TO_THERAPY,
        MAD_FILLS_180D,
        MAD_NTT_RESTART,
        MAD_LATEST_FILL_DAYS_SUPPLY,
        MAD_LATEST_FILL_QUANTITY,
        MAC_FILLS_2Y,
        MAC_LATEST_FILL_DATE,
        MAC_DAYS_SINCE_LAST_FILL,
        MAC_NEW_TO_THERAPY,
        MAC_FILLS_180D,
        MAC_NTT_RESTART,
        MAC_LATEST_FILL_DAYS_SUPPLY,
        MAC_LATEST_FILL_QUANTITY,
        MAH_FILLS_2Y,
        MAH_LATEST_FILL_DATE,
        MAH_DAYS_SINCE_LAST_FILL,
        MAH_NEW_TO_THERAPY,
        MAH_FILLS_180D,
        MAH_NTT_RESTART,
        MAH_LATEST_FILL_DAYS_SUPPLY,
        MAH_LATEST_FILL_QUANTITY,
        MBI,
        ENCRYPT,
        REPORT_DATE,
        OC_FLAG,
        EMR_FLAG,
        HC_OC_COMANAGED_FLAG,
        ENTERPRISE_INDV_ID,
        CLAIMS_REFRESH_DATE,
        MAC_NTT_GENERIC,
        MAD_NTT_GENERIC,
        MAH_NTT_GENERIC,
        MAC_NTT_MED_NAME,
        MAD_NTT_MED_NAME,
        MAH_NTT_MED_NAME,
        PROCESS_LOG_ID,
        LOAD_DATETIME,
        RECORD_ID,
        USER_STORY
    FROM FINAL_DATA
    ORDER BY LOAD_DATETIME, RECORD_ID;

END;
GO
