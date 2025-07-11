from sqlglot import parse_one, exp, condition
from sqlglot.expressions import Select, AggFunc, Column, Alias, Group
#from pathlib import Path

#path_to_db = Path(__file__).parent / 'data' / 'sqlite.db'

def add_txt_fields(sql_query):
    try:
        parsed = parse_one(sql_query)
    except Exception:
        return sql_query
    #print(parsed.selects)
    for select_expr in parsed.selects:         
        if isinstance(select_expr, Column) and select_expr.name not in ('VBRK_VBELN', "VBRP_POSNR", "VBRK_FKDAT", 
                                                                        "ZQSHIPTOF", "ZAREVENF_RUB", "ZAMARGPRF_RUB", 
                                                                        "KONV_KWERT_ZVUC_RUB",
                                                                        ): #хардкод
            txt_field = f'{select_expr.name}_TXT'
            parsed.selects.append(txt_field) #add to select
            if parsed.args.get("group"):
                pass
                parsed.args["group"].append("expressions", txt_field) #add to group by
    
    return parsed.sql(pretty=True, identify=True)

if __name__ == "__main__":
    test_query = """
SELECT SUM(ZQSHIPTOF) as Q, "ZCFO1", "ZCFO2"
FROM SAPABAP1.ZZSDM_117_CUST
GROUP BY ZCFO1;
"""
    test = add_txt_fields(test_query)
    print(test)