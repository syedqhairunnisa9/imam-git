SELECT
    DISTINCT B.BID_GEO_CD,
    COALESCE(B.REGION, B.BID_GEO_CD) AS REGION,
    B.CUST_CTRY_NAME AS COUNTRY,
    B.BRAND,
    B.CFP,
    A.BID_ID,
    B.CUST_NAME AS CUSTOMER_NAME,
    CAST(
        ROUND((B.PRICE_TO_BP) / 1000000, 2) AS DECIMAL(19, 2)
    ) AS REVENUE,
    CASE
        when LOCATE('×', B.BID_FACTOR_LIST) = 0 THEN REPLACE(
            REPLACE(
                REPLACE(
                    UPPER(B.BID_FACTOR_LIST),
                    'CONFIDENTIALITY',
                    'NDA'
                ),
                ':',
                ', '
            ),
            'FOCUS BP',
            'Focus BP'
        )
        WHEN UPPER(B.BID_FACTOR_LIST) LIKE '%FOCUS BP%' THEN REPLACE(
            REPLACE(
                UPPER(
                    REPLACE(
                        REPLACE(
                            concat(
                                LEFT(
                                    B.BID_FACTOR_LIST,
                                    LOCATE(':', B.BID_FACTOR_LIST) -1
                                ),
                                REPLACE(
                                    SUBSTR(
                                        B.BID_FACTOR_LIST,
                                        varchar(LOCATE(';×', B.BID_FACTOR_LIST) + 1),
                                        LENGTH(varchar(B.BID_FACTOR_LIST)) + 1
                                    ),
                                    ';×',
                                    '×'
                                )
                            ),
                            '×',
                            ', '
                        ),
                        'FOCUS BP',
                        'Focus BP'
                    )
                ),
                'CONFIDENTIALITY',
                'NDA'
            ),
            'FOCUS BP',
            'Focus BP'
        )
        ELSE REPLACE(
            REPLACE(
                UPPER(REPLACE(B.BID_FACTOR_LIST, '×', ', ')),
                'CONFIDENTIALITY',
                'NDA'
            ),
            ',',
            ', '
        )
    END BID_FACTOR_LIST,
    B.AGING,
    M.EXECUTIVE_LOG
from
    BRTRMD.V_RMD_TRACKER_RPT B
    LEFT OUTER JOIN (
        select
            bid_id,
            max(tracker_id) tracker_id
        from
            BRTRMD.V_RMD_TRACKER_RPT
        group by
            bid_id
    ) A ON B.bid_id = A.bid_id
    AND B.tracker_id = A.tracker_id,
    (
        select
            bid_id,
            cast(
                LISTAGG(cast(executive_log AS VARCHAR(20000)), ',') as varchar(20000)
            ) as EXECUTIVE_LOG,
            tracker_id
        from
            (
                select
                    bid_id,
                    case
                        when executive_log is not null
                        and trim(executive_log) like '__/__/%' then replace(
                            executive_log,
                            substr(trim(executive_log), 6, 5),
                            ' '
                        )
                        when executive_log is not null
                        and trim(executive_log) like '_/_/%' then replace(
                            executive_log,
                            substr(trim(executive_log), 4, 5),
                            ' '
                        )
                        when executive_log is not null
                        and trim(executive_log) like '_/__/%' then replace(
                            executive_log,
                            substr(trim(executive_log), 5, 5),
                            ' '
                        )
                        when executive_log IS NOT NULL
                        AND UPPER(EXECUTIVE_LOG) LIKE UPPER('0%') THEN REPLACE(executive_log, SUBSTR(executive_log, 6, 5), ' ')
                        ELSE executive_log
                    END AS executive_log,
                    tracker_id
                from
                    (
                        select
                            bid_id,
                            tracker_id,
                            section_id,
                            max(action_id),
                            max(log_date),
                            executive_log
                        from
                            BRTRMD.V_RMD_TRACKER_RPT
                        where
                            (bid_id, tracker_id) in (
                                select
                                    bid_id,
                                    max(tracker_id)
                                from
                                    BRTRMD.V_RMD_TRACKER_RPT
                                group by
                                    bid_id
                            )
                        group by
                            bid_id,
                            tracker_id,
                            section_id,
                            executive_log
                    )
            ) C
        GROUP BY
            C.BID_ID,
            tracker_id
    ) M
WHERE
    B.NEXT_MONTH_BID = 'N'
    AND (
        B.STATUS = 'OPEN'
        and (
            (
                timestampdiff(
                    8,
                    char(CURRENT_TIMESTAMP - timestamp(B.CREATED_DATE))
                )
            ) > 24
        )
    )
    and B.PRICE_TO_BP > 250000
    AND B.bid_id = M.bid_id
    AND B.tracker_id = M.tracker_id
    and A.bid_id = M.bid_id
    and A.tracker_id = M.tracker_id