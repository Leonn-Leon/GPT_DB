examples = [
#простые примеры
    {
        "question": "Покажи фактуры",
        "answer": "select distinct VBRK_VBELN from ZZSDM_117_CUST"
    },
    {
        "question": "Покажи фактуры и позиции",
        "answer": "select VBRK_VBELN, VBRP_POSNR from ZZSDM_117_CUST"
    },
    {
        "question": "Покажи отгрузки",
        "answer": "select distinct VBRK_VBELN, VBRP_POSNR, ZQSHIPTOF  from ZZSDM_117_CUST"
    },
    {
        "question": "Покажи общую маржинальную прибыль",
        "answer": "select sum(ZAMARGPRF_RUB) from ZZSDM_117_CUST"
    },
    {
        "question": "Покажи фактуры и количество позиций",
        "answer": "SELECT VBRK_VBELN, COUNT(VBRP_POSNR) AS Positions_Count FROM ZZSDM_117_CUST GROUP BY VBRK_VBELN"
    },
    {
      "question": "Покажи список клиентов",
      "answer": "SELECT DISTINCT ZCUSTOMER FROM ZZSDM_117_CUST"
    },
    {
      "question": "Покажи топ 5 самых дорогих позиций",
      "answer": "SELECT TOP 5 VBRK_VBELN, VBRP_POSNR, VBRP_NETWR FROM ZZSDM_117_CUST ORDER BY VBRP_NETWR DESC"
    },
    {
      "question": "Покажи среднюю маржу по позициям",
      "answer": "SELECT AVG(ZAMARGPRF_RUB) AS Avg_Margin FROM ZZSDM_117_CUST"
    },
    {
      "question": "Покажи среднюю маржу по фактурам",
      "answer": "SELECT AVG(ZAMARGPRF_RUB) AS Avg_Margin FROM (select sum(ZAMARGPRF_RUB) from ZZSDM_117_CUST group by VBRK_VBELN) as t"
    },
    {
      "question": "Покажи среднюю маржу по отгрузкам",
      "answer": "SELECT AVG(ZAMARGPRF_RUB) AS Avg_Margin FROM (select sum(ZAMARGPRF_RUB) from ZZSDM_117_CUST group by VBRK_VBELN) as t"
    },
    {
      "question": "Покажи маржу по отгрузкам",
      "answer": "SELECT VBRK_VBELN, sum(ZAMARGPRF_RUB) AS Avg_Margin FROM ZZSDM_117_CUST group by VBRK_VBELN"
    },
    {
      "question": "Покажи отгруженные материалы и их количества в кг",
      "answer": "SELECT VBRP_MATNR, sum(FKIMG_KG) FROM ZZSDM_117_CUST group by VBRP_MATNR"
    },
    {
      "question": "Покажи сумму выручки по каждому каналу сбыта",
      "answer": "SELECT VBRK_VTWEG, SUM(VBRP_NETWR) FROM ZZSDM_117_CUST GROUP BY VBRK_VTWEG"
    },
    {
      "question": "Покажи отгрузку с большим количество позиций",
      "answer": "SELECT TOP 1 VBRK_VBELN, count(VBRP_POSNR) FROM ZZSDM_117_CUST group by VBRK_VBELN ORDER BY count(VBRP_POSNR) DESC"
    },

    {
        "question": "Покажи топ 3 менеджера по количеству отгрузок",
        "answer": "SELECT TOP 3 VBRK_ZZPERNR_ZM, count(distinct VBRK_VBELN) from ZZSDM_117_CUST group by VBRK_ZZPERNR_ZM order by count(distinct VBRK_VBELN) desc"
    },




#примеры с ограничениями. 
    {
        "question": "Покажи количество отгруженных клиентов в уральском дивизионе",
        "answer": "select count(distinct ZCUSTOMER) from ZZSDM_117_CUST where ZDIV = '02' "
    },
    {
        "question": "Покажи количество отгрузок в сибирском дивизионе",
        "answer": "select count(distinct VBRK_VBELN) from ZZSDM_117_CUST where ZDIV = '04' "
    },
    {
        "question": "Покажи отгрузку с максимальной маржинальной прибылью в поволжском дивизионе",
        "answer": "select TOP 1 VBRK_VBELN, sum(ZAMARGPRF_RUB) from ZSDM_117  group by VBRK_VBELN order by sum(ZAMARGPRF_RUB) desc"
    },
    {
        "question": "Покажи выручку в уральском и сибирском дивизионе",
        "answer": "select ZDIV, sum(VBRP_NETWR) from ZZSDM_117_CUST where ZDIV in ('02', '04') group by ZDIV"
    },
    {
        "question": "Покажи количество обычных сделок",
        "answer": "select count(distinct VBRK_VBELN) from ZZSDM_117_CUST where TYPE_ORDER = 'обычная'"
    },
    {
        "question": "сколько было отгружено по экмпортным сделкам",
        "answer": "select sum(ZQSHIPTOF) from ZZSDM_117_CUST where TYPE_ORDER = 'экспорт'"
    },
    {
        "question": "сколько было отгружено по категории тмп",
        "answer": "select sum(ZQSHIPTOF) from ZZSDM_117_CUST where ZPROD_CAT = 'тмп'"
    },
    {
        "question": "сколько b2b клиентов было отгружено",
        "answer": "select count(distinct ZCUSTOMER) from ZZSDM_117_CUST where BUT000_BPKIND = 'b2b'"
    },




#примеры с датами
    {
      "question": "Выведи фактуры за 2024 год",
      "answer": "SELECT count(distinct VBRK_VBELN) FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 4) = '2024'"
    },
    {
      "question": "Покажи позиции, отгруженные за последнюю неделю",
      "answer": "SELECT VBRK_VBELN, VBRP_POSNR FROM ZZSDM_117_CUST WHERE VBRK_FKDAT >= to_varchar(add_days(CURRENT_DATE, -7), 'YYYYMMDD')"
    },
    {
      "question": "В каком месяце 2023 года была самая высокая выручка?",
      "answer": "SELECT TOP 1 left(VBRK_FKDAT, 6) AS Month, SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 4) = '2023' GROUP BY left(VBRK_FKDAT, 6) ORDER BY SUM(VBRP_NETWR) DESC"
    },
    {
      "question": "Сколько клиентов отгрузилось в январе?",
      "answer": "SELECT count(distinct ZCUSTOMER) FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYY') || '01'"
    },
    {
        "question": "Покажи распределение выручки по кварталам за 2024 год",
        "answer": "select QUARTER(VBRK_FKDAT), sum(VBRP_NETWR) from ZZSDM_117_CUST where left(VBRK_FKDAT, 4) = '2024' group by quarter(VBRK_FKDAT)"
    },
    {
     "question": "Покажи выручку за период января 2023 года по май 2024 года",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 6) BETWEEN '202301' AND '202305'"
    },
    {
     "question": "Покажи выручку за второе мая",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE VBRK_FKDAT = to_varchar(CURRENT_DATE, 'YYYY') || '0502'"
    },
    {
     "question": "Покажи выручку за вчера",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE VBRK_FKDAT = to_varchar(add_days(CURRENT_DATE, -1), 'YYYYMMDD')"
    },
    {
     "question": "Покажи выручку за первый квартал",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE QUARTER(VBRK_FKDAT) = to_varchar(CURRENT_DATE, 'YYYY') || '-Q1'"
    },
    {
     "question": "Покажи выручку за второй квартал",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE QUARTER(VBRK_FKDAT) = to_varchar(CURRENT_DATE, 'YYYY') || '-Q2'"
    },
    {
     "question": "Покажи выручку за текущий квартал",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE QUARTER(VBRK_FKDAT) = QUARTER(CURRENT_DATE)"
    },
    {
     "question": "Покажи выручку с первого по пятое марта",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE VBRK_FKDAT BETWEEN to_varchar(CURRENT_DATE, 'YYYY') || '0301' AND to_varchar(CURRENT_DATE, 'YYYY') || '0305'"
    },




# остальные (не вошли в группы)
    {
        "question": "Покажи маржу за текущий месяц по филиалам с кодам 01 и 02",
        "answer": "select ZCFO1, sum(ZAMARGPRF_RUB) from ZSDM_117 where ZCFO in ('01', '02') and left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by ZCFO1"
    },
    {
        "question": "Покажи маржу за месяц по филиалам с кодами 01 и 02",
        "answer": "select ZCFO1, sum(ZAMARGPRF_RUB) from ZSDM_117 where ZCFO in ('01', '02') and left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by ZCFO1"
    },
    {
        "question": "Покажи маржу за прошлый год",
        "answer": "select sum(ZAMARGPRF_RUB) from ZSDM_117 where left(VBRK_FKDAT, 4) = to_varchar(add_year(CURRENT_DATE, -1), 'YYYY')"
    },
    {
        "question": "Покажи отгрузки с максимальной маржинальной прибылью за прошлый месяц",
        "answer": "select VBRK_VBELN, sum(ZAMARGPRF_RUB) from ZSDM_117 where left(VBRK_FKDAT, 6) = to_varchar(add_months(CURRENT_DATE, -1), 'YYYYMM') group by VBRK_VBELN"
    },
    {
        "question": "Покажи отгрузки с максимальным количеством позиций в первом месяце этого года",
        "answer": "select TOP 1 VBRK_VBELN, count(VBRP_POSNR) from ZSDM_117 where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYY') || '01' group by VBRK_VBELN order by count(VBRP_POSNR) desc"
    },
    {
        "question": "Покажи позицию с самой низкой выручкой за этот месяц",
        "answer": "select TOP 1 VBRP_POSNR, VBRP_NETWR from ZSDM_117 where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') order by count(VBRP_POSNR) asc"
    },
    {
        "question": "Покажи отгрузку с самой низкой выручкой за этот месяц",
        "answer": "select TOP 1 VBRK_VBELN, sum(VBRP_NETWR) from ZSDM_117 where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by VBRK_VBELN order by sum(VBRP_NETWR) desc"
    },
    {
        "question": "Покажи выручку и количество фактур за сегодня по всем дивизионам",
        "answer": "select zdiv, sum(VBRP_NETWR), count(distinct VBRK_VBELN) from ZSDM_117 where VBRK_FKDAT = to_varchar(CURRENT_DATE, 'YYYYMMDD') group by zdiv"
    },
    {
        "question": "Покажи АГ3 с самой высокой средней себестоимостью в этом месяце",
        "answer": "select TOP 1 ZPRODH31, avg(KONV_KWERT_ZVUC_RUB) from ZSDM_117 where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by ZPRODH31 order by avg(KONV_KWERT_ZVUC_RUB) desc"
    },
]