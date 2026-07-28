-- Compute & context
USE WAREHOUSE WH_INGEST;
USE DATABASE SEC_PRICING;

-- Fail if nothing arrived for the given TRADE_DATE
SELECT COUNT(*) > 0
FROM RAW.RAW_EOD_PRICES
WHERE TRADE_DATE = TO_DATE('{{ ti.xcom_pull(task_ids=params.trading_ds_task_id, key="trading_date") }}')

