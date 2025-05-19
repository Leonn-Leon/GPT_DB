examples = [
#простые примеры
    {
        "question": "Покажи фактуры",
        "answer": "select distinct VBRK_VBELN from ZZSDM_117_CUST",
        "comment": "Список уникальных фактур фактур <VBRK_VBELN>:"
    },
    {
        "question": "Покажи фактуры и позиции",
        "answer": "select VBRK_VBELN, VBRP_POSNR from ZZSDM_117_CUST",
        "comment": "Список фактур <VBRK_VBELN> и позиций <VBRP_POSNR>:"
    },
    {
        "question": "Покажи общую маржинальную прибыль",
        "answer": "select sum(ZAMARGPRF_RUB) as ZAMARGPRF_RUB from ZZSDM_117_CUST",
        "comment": "Общая маржинальная прибыль составила <ZAMARGPRF_RUB>"
    },
    {
        "question": "Покажи фактуры и количество позиций",
        "answer": "SELECT VBRK_VBELN, COUNT(VBRP_POSNR) AS Positions_Count FROM ZZSDM_117_CUST GROUP BY VBRK_VBELN",
        "comment": "Общая маржинальная прибыль составила <ZAMARGPRF_RUB>"
    },
    {
      "question": "Покажи список клиентов",
      "answer": "SELECT DISTINCT ZCUSTOMER, ZCUSTOMER_TXT FROM ZZSDM_117_CUST",
      "comment": "Список уникальных клиентов <ZCUSTOMER_TXT>:"
    },
    {
      "question": "Покажи топ 5 самых дорогих позиций",
      "answer": "SELECT TOP 5 VBRK_VBELN, VBRP_POSNR, VBRP_NETWR FROM ZZSDM_117_CUST ORDER BY VBRP_NETWR DESC",
      "comment": "Список из 5 фактур <VBRK_VBELN> и позиций <VBRP_POSNR> с самой высокой выручкой VBRP_NETWR:"
    },
    {
      "question": "Покажи среднюю маржу по позициям",
      "answer": "SELECT AVG(ZAMARGPRF_RUB) AS ZAMARGPRF_RUB FROM ZZSDM_117_CUST",
      "comment": "Средняя маржа по позициям составила <ZAMARGPRF_RUB>"
    },
    {
      "question": "Покажи среднюю маржу по фактурам",
      "answer": "SELECT AVG(ZAMARGPRF_RUB) AS ZAMARGPRF_RUB FROM (select sum(ZAMARGPRF_RUB) from ZZSDM_117_CUST group by VBRK_VBELN) as t",
      "comment": "Средняя маржа по фактурам составила <ZAMARGPRF_RUB>"
    },
    {
      "question": "Покажи среднюю маржу по отгрузкам",
      "answer": "SELECT AVG(ZAMARGPRF_RUB) AS ZAMARGPRF_RUB FROM (select sum(ZAMARGPRF_RUB) from ZZSDM_117_CUST group by VBRK_VBELN) as t",
      "comment": "Средняя маржа по фактурам составила <ZAMARGPRF_RUB>"
    },
    {
      "question": "Покажи маржу по отгрузкам",
      "answer": "SELECT VBRK_VBELN, sum(ZAMARGPRF_RUB) AS Avg_Margin FROM ZZSDM_117_CUST group by VBRK_VBELN",
      "comment": "Список из фактур <VBRK_VBELN> и маржи <ZAMARGPRF_RUB> по этим фактурам:"
    },
    {
      "question": "Покажи отгрузку с большим количество позиций",
      "answer": "SELECT TOP 1 VBRK_VBELN, count(VBRP_POSNR) VBRP_POSNR FROM ZZSDM_117_CUST group by VBRK_VBELN ORDER BY count(VBRP_POSNR) DESC",
      "comment": "Отгрузка <VBRK_VBELN> имеет <VBRP_POSNR> позиций. Это самое большое количество позиций для отгрузки"
    },

    {
        "question": "Покажи топ 3 менеджера по количеству отгрузок",
        "answer": "SELECT TOP 3 VBRK_ZZPERNR_ZM, VBRK_ZZPERNR_ZM_TXT, count(distinct VBRK_VBELN) from ZZSDM_117_CUST group by VBRK_ZZPERNR_ZM, VBRK_ZZPERNR_ZM_TXT order by count(distinct VBRK_VBELN) desc",
        "comment": "Список из 3 менеджеров <VBRK_ZZPERNR_ZM_TXT> по количеству отгрузок:"
    },




#примеры с ограничениями. 
    {
        "question": "Покажи количество отгруженных клиентов в уральском дивизионе",
        "answer": "select count(distinct ZCUSTOMER) from ZZSDM_117_CUST ZDIV = '02'",
        "comment": "Список отгруженных клиентов <ZCUSTOMER> в уральском дивизионе:"
    },
    {
        "question": "Покажи количество отгруженных клиентов в филиале спк Владивосток",
        "answer": "select count(distinct ZCUSTOMER) from ZZSDM_117_CUST ZCFO1 = '0601'",
        "comment": "Список отгруженных клиентов <ZCUSTOMER> в филиале спк Владивосток:"
    },
    {
        "question": "Покажи количество отгрузок в сибирском дивизионе",
        "answer": "select count(distinct VBRK_VBELN) from ZZSDM_117_CUST ZDIV = '04' ",
        "comment": "Список фактур <VBRK_VBELN> в сибирском дивизионе:"
    },
    {
        "question": "Покажи количество отгрузок в филиале смц иркутск",
        "answer": "select count(distinct VBRK_VBELN) as COUNT_OF_VBRK_VBELN from ZZSDM_117_CUST WHERE ZCFO1 = '1002' ",
        "comment": "В филиале СМЦ Иркутск было <COUNT_OF_VBRK_VBELN> отгрузок"
    },
    {
        "question": "Покажи отгрузку с максимальной маржинальной прибылью в поволжском дивизионе",
        "answer": "select TOP 1 VBRK_VBELN, sum(ZAMARGPRF_RUB) as ZAMARGPRF_RUB from ZZSDM_117_CUST ZDIV = '03' group by VBRK_VBELN order by sum(ZAMARGPRF_RUB) desc",
        "comment": "Отгрузка <VBRK_VBELN> имеет маржинальную прибыль <ZAMARGPRF_RUB>. Это самая большая маржинальная прибыль у отгрузки в Поволжском дивизионе "
    },
    {
        "question": "Покажи выручку в уральском и сибирском дивизионе",
        "answer": "select ZDIV, ZDIV_TXT sum(VBRP_NETWR) from ZZSDM_117_CUST WHERE ZDIV in ('02', '04') group by ZDIV, ZDIV_TXT",
        "comment": "Список выручки <VBRP_NETWR> по Уральскому и Сибирскому дивизионам"
    },
    {
        "question": "Покажи выручку в филиалах смц Абакан и спк Абакан",
        "answer": "select ZCFO1, ZCFO1_TXT sum(VBRP_NETWR) from ZZSDM_117_CUST WHERE ZCFO1 in ('0201', '0202') group by ZCFO1, ZCFO1_TXT",
        "comment": "Список выручки <VBRP_NETWR> по филиалам смц Абакан и спк Абакан"
    },
    {
        "question": "Покажи количество отгрузок с обычным видом сделки",
        "answer": "select count(distinct VBRK_VBELN) COUNT_OF_VBRK_VBELN from ZZSDM_117_CUST WHERE UPPER(TYPE_ORDER_TXT) LIKE 'ОБЫЧНАЯ'",
        "comment": "Было <COUNT_OF_VBRK_VBELN> отгрузок с обычным видом сделки"
    },
    {
        "question": "сколько тонн было отгружено по экспортным сделкам",
        "answer": "select sum(ZQSHIPTOF) as ZQSHIPTOF from ZZSDM_117_CUST where UPPER(TYPE_ORDER_TXT) LIKE 'ЭКСПОРТ'",
        "comment": "Было отгружено <ZQSHIPTOF> тонн по экспортным сделкам"
    },
    {
        "question": "сколько килограмм было отгружено по экспортным сделкам",
        "answer": "select sum(ZQSHIPTOF) * 1000 as ZQSHIPTOF_KG from ZZSDM_117_CUST where UPPER(TYPE_ORDER_TXT) LIKE 'ЭКСПОРТ'",
        "comment": "Было отгружено <ZQSHIPTOF_KG> килограмм по экспортным сделкам"
    },
    {
        "question": "сколько было отгружено по категории материала тмп",
        "answer": "select sum(ZQSHIPTOF) as ZQSHIPTOF from ZZSDM_117_CUST where ZPROD_CAT = '02'",
        "comment": "Было отгружено <ZQSHIPTOF> тонн по экспортным сделкам"
    },
    {
        "question": "сколько b2b клиентов было отгружено",
        "answer": "select count(distinct ZCUSTOMER) as COUNT_OF_ZCUSTOMER from ZZSDM_117_CUST where UPPER(BUT000_BPKIND) = 'B2B'",
        "comment": "Было отгружено <COUNT_OF_ZCUSTOMER> b2b клиентов"
    },
    {
        "question": "покажи общую выручку по отрасли машиностроение по филиалу спк екатеринбург",
        "answer": "select sum(VBRP_NETWR) as VBRP_NETWR from ZZSDM_117_CUST WHERE INDUSTRY = '32' and ZCFO1 = '0802'",
        "comment": "Общая выручка по отрасли машиностроение по филиалу спк екатеринбург составила <VBRP_NETWR>"
    },
    {
        "question": "сколько было отгрузок в 2024 году у менеджера Наумовой Ольги Павловны?",
        "answer": "select count(distinct VBRK_VBELN) as COUNT_OF_VBRK_VBELN from ZZSDM_117_CUST where VBRK_ZZPERNR_ZM = '20' and left(VBRK_FKDAT, 4) = '2024'",
        "comment": "В 2024 году у менеджера Наумовой Ольги Павловны было <COUNT_OF_VBRK_VBELN> отгрузок"
    },
    {
        "question": "сколько было отгрузок в 2024 году у менеджера Барышниковой Натальи Андреевны?",
        "answer": "select count(distinct VBRK_VBELN) from ZZSDM_117_CUST where VBRK_ZZPERNR_ZM = '20' and left(VBRK_FKDAT, 4) = '2024'",
        "comment": "В 2024 году у менеджера Барышниковой Натальи Андреевны было <COUNT_OF_VBRK_VBELN> отгрузок"
    },
    {
        "question": "сколько было отгрузок с иерархией нулевого уровня трубой?",
        "answer": "select count(distinct VBRK_VBELN) as COUNT_OF_VBRK_VBELN from ZZSDM_117_CUST where ZPRODH01 = '20",
        "comment": "С иерархией нулевого уровня трубой было <COUNT_OF_VBRK_VBELN> отгрузок"
    },
    {
        "question": "сколько было отгрузок с иерархией первого уровня Труба Б Ш?",
        "answer": "select count(distinct VBRK_VBELN) as COUNT_OF_VBRK_VBELN from ZZSDM_117_CUST where ZPRODH11 = '20'",
        "comment": "С иерархией первого уровня Трубой Б Ш было <COUNT_OF_VBRK_VBELN> отгрузок"
    },
    {
        "question": "сколько было отгрузок с иерархией второго уровня Арматурой 32?",
        "answer": "select count(distinct VBRK_VBELN) as COUNT_OF_VBRK_VBELN from ZZSDM_117_CUST where ZPRODH21 = '20'",
        "comment": "С иерархией второго уровня Арматурой 32 было <COUNT_OF_VBRK_VBELN> отгрузок"
    },
    {
        "question": "когда была первая отгрузка клиента крепёжные системы?",
        "answer": "select min(VBRK_FKDAT) as VBRK_FKDAT from ZZSDM_117_CUST where ZCUSTOMER = '20'",
        "comment": "Первая отгрузка клиента крепёжные системы была <VBRK_FKDAT>"
    },
    {
        "question": "когда была последняя отгрузка клиента с кодом 9000398679?",
        "answer": "select max(VBRK_FKDAT) as VBRK_FKDAT from ZZSDM_117_CUST where ZCUSTOMER = '9000398679'",
        "comment": "Последняя отгрузка клиента с кодом 9000398679 была <VBRK_FKDAT>"
    },
    {
        "question": "когда была последняя отгрузка с кодом иерархии нулевого уровня 940?",
        "answer": "select max(VBRK_FKDAT) as VBRK_FKDAT from ZZSDM_117_CUST where ZPRODH01 = '940'",
        "comment": "Последняя отгрузка с кодом иерархии нулевого уровня 940 была <VBRK_FKDAT>"
    },

#примеры с датами
    {
      "question": "Выведи фактуры за 2024 год",
      "answer": "SELECT count(distinct VBRK_VBELN) FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 4) = '2024'",
      "comment": "Список уникальных фактура <VBRK_VBELN> за 2024 год:"
    },
    {
      "question": "Покажи фактуры и позиции, отгруженные за последнюю неделю",
      "answer": "SELECT VBRK_VBELN, VBRP_POSNR FROM ZZSDM_117_CUST WHERE VBRK_FKDAT >= to_varchar(add_days(CURRENT_DATE, -7), 'YYYYMMDD')",
      "comment": "Список фактур <VBRK_VBELN> и позиций за последнюю неделю:"
    },
    {
      "question": "В каком месяце 2023 года была самая высокая выручка?",
      "answer": "SELECT TOP 1 left(VBRK_FKDAT, 6) AS Month, SUM(VBRP_NETWR) AS VBRP_NETWR FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 4) = '2023' GROUP BY left(VBRK_FKDAT, 6) ORDER BY SUM(VBRP_NETWR) DESC",
      "comment": "В месяце <Month> была выручка <VBRP_NETWR>. Это самая большая выручка в 2023 году."
    },
    {
      "question": "Сколько клиентов отгрузилось в январе?",
      "answer": "SELECT count(distinct ZCUSTOMER) COUNT_OF_ZCUSTOMER FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYY') || '01'",
      "comment": "В январе отгрузилось <COUNT_OF_ZCUSTOMER> клиентов"
    },
    {
        "question": "Покажи распределение выручки по кварталам за 2024 год",
        "answer": "select QUARTER(VBRK_FKDAT), sum(VBRP_NETWR) from ZZSDM_117_CUST where left(VBRK_FKDAT, 4) = '2024' group by quarter(VBRK_FKDAT)",
        "comment": "Список распределения выручки <VBRK_FKDAT> по кварталам за 2024 год:"
    },
    {
     "question": "Покажи выручку за период января 2023 года по май 2024 года",
     "answer": "SELECT SUM(VBRP_NETWR) as VBRP_NETWR FROM ZZSDM_117_CUST WHERE left(VBRK_FKDAT, 6) BETWEEN '202301' AND '202305'",
     "comment": "С января 2023 года по май 2024 года выручка составила VBRP_NETWR"
    },
    {
     "question": "Покажи выручку за второе мая",
     "answer": "SELECT SUM(VBRP_NETWR) as VBRP_NETWR FROM ZZSDM_117_CUST WHERE VBRK_FKDAT = to_varchar(CURRENT_DATE, 'YYYY') || '0502'",
     "comment": "За 2 мая выручка составила VBRP_NETWR"
    },
    {
     "question": "Покажи выручку за вчера",
     "answer": "SELECT SUM(VBRP_NETWR) as VBRP_NETWR FROM ZZSDM_117_CUST WHERE VBRK_FKDAT = to_varchar(add_days(CURRENT_DATE, -1), 'YYYYMMDD')",
     "comment": "За вчера выручка составила VBRP_NETWR"
    },
    {
     "question": "Покажи выручку за первый квартал",
     "answer": "SELECT SUM(VBRP_NETWR) as VBRP_NETWR FROM ZZSDM_117_CUST WHERE QUARTER(VBRK_FKDAT) = to_varchar(CURRENT_DATE, 'YYYY') || '-Q1'",
     "comment": "За первый квартал выручка составила VBRP_NETWR"
    },
    {
     "question": "Покажи выручку за второй квартал",
     "answer": "SELECT SUM(VBRP_NETWR) as VBRP_NETWR FROM ZZSDM_117_CUST WHERE QUARTER(VBRK_FKDAT) = to_varchar(CURRENT_DATE, 'YYYY') || '-Q2'",
     "comment": "За второй квартал выручка составила VBRP_NETWR"
    },
    {
     "question": "Покажи выручку за текущий квартал",
     "answer": "SELECT SUM(VBRP_NETWR) as VBRP_NETWR FROM ZZSDM_117_CUST WHERE QUARTER(VBRK_FKDAT) = QUARTER(CURRENT_DATE)",
     "comment": "За текущий квартал выручка составила VBRP_NETWR"
    },
    {
     "question": "Покажи выручку с первого по пятое марта",
     "answer": "SELECT SUM(VBRP_NETWR) FROM ZZSDM_117_CUST WHERE VBRK_FKDAT BETWEEN to_varchar(CURRENT_DATE, 'YYYY') || '0301' AND to_varchar(CURRENT_DATE, 'YYYY') || '0305'",
     "comment": "С первого по пятое марта выручка составила VBRP_NETWR"
    },




# остальные (не вошли в группы)
    {
        "question": "Покажи маржу за текущий месяц по филиалам СПК-Казань и СМЦ-Иркутск",
        "answer": "select ZCFO1, ZCFO1_TXT, sum(ZAMARGPRF_RUB) from ZZSDM_117_CUST ZCFO1 in ('1002', '1101') and left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by ZCFO1, ZCFO1_TXT",
        "comment": "Список маржинальной прибыли <ZAMARGPRF_RUB> по филиалам СПК-Казань и СМЦ-Иркутск за текущий месяц:"
    },
    {
        "question": "Покажи маржу за прошлый год",
        "answer": "select sum(ZAMARGPRF_RUB) as ZAMARGPRF_RUB from ZZSDM_117_CUST where left(VBRK_FKDAT, 4) = to_varchar(add_year(CURRENT_DATE, -1), 'YYYY')",
        "comment": "За прошлый год маржинальная прибыль составила <ZAMARGPRF_RUB>"
    },
    {
        "question": "Покажи отгрузку с максимальной маржинальной прибылью за прошлый месяц",
        "answer": "select TOP1 VBRK_VBELN, sum(ZAMARGPRF_RUB) as ZAMARGPRF_RUB from ZZSDM_117_CUST where left(VBRK_FKDAT, 6) = to_varchar(add_months(CURRENT_DATE, -1), 'YYYYMM') group by VBRK_VBELN order by sum(ZAMARGPRF_RUB) desc",
        "comment": "Для фактуры <VBRK_VBELN> маржинальная прибыль составила <ZAMARGPRF_RUB>. Это самая высокая маржинальная прибыль за прошлый месяц."
    },
    {
        "question": "Покажи отгрузку с максимальным количеством позиций в первом месяце этого года",
        "answer": "select TOP 1 VBRK_VBELN, count(VBRP_POSNR) as COUNT_OF_VBRP_POSNR from ZZSDM_117_CUST where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYY') || '01' group by VBRK_VBELN order by count(VBRP_POSNR) desc",
        "comment": "Фактура <VBRK_VBELN> имела <COUNT_OF_VBRP_POSNR> позиций. Это максимальное колечество позиций для фактуры в январе этого года."
    },
    {
        "question": "Покажи позицию с самой низкой выручкой за этот месяц",
        "answer": "select TOP 1 VBRK_VBELN, VBRP_POSNR, VBRP_NETWR from ZZSDM_117_CUST where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') order by VBRP_NETWR asc",
        "comment": "Позиция <VBRP_POSNR> у фактуры <VBRK_VBELN> имела выручку VBRP_NETWR. Это позиция с самой низкой выручкой за текущий месяц"
    },
    {
        "question": "Покажи отгрузку с самой низкой выручкой за этот месяц",
        "answer": "select TOP 1 VBRK_VBELN, sum(VBRP_NETWR) as VBRP_NETWR from ZZSDM_117_CUST where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by VBRK_VBELN order by sum(VBRP_NETWR) desc",
        "comment": "Отгрузка <VBRK_VBELN> имела выручку <VBRP_NETWR>. Это самая низкая выручка за текущий месяц"
    },
    {
        "question": "Покажи выручку и количество фактур за сегодня по всем дивизионам",
        "answer": "select zdiv, sum(VBRP_NETWR), count(distinct VBRK_VBELN) from ZZSDM_117_CUST where VBRK_FKDAT = to_varchar(CURRENT_DATE, 'YYYYMMDD') group by zdiv",
        "comment": "Список выручки <VBRP_NETWR> и колечества фактур <VBRK_VBELN> по всем дивизионам за текущую дату"
    },
    {
        "question": "Покажи АГ3 с самой высокой средней себестоимостью в этом месяце",
        "answer": "select TOP 1 ZPRODH31, ZPRODH31_TXT, avg(KONV_KWERT_ZVUC_RUB) as KONV_KWERT_ZVUC_RUB from ZZSDM_117_CUST where left(VBRK_FKDAT, 6) = to_varchar(CURRENT_DATE, 'YYYYMM') group by ZPRODH31, ZPRODH31_TXT order by avg(KONV_KWERT_ZVUC_RUB) desc",
        "comment": "АГ3 <ZPRODH31> имеет среднюю себестоимость <KONV_KWERT_ZVUC_RUB>. Это самая высокая средняя себестоимость для АГ3 в текущем месяце"
    },
]