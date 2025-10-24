import snowflake.connector
from datetime import datetime

today_str = datetime.now().strftime("%Y%m%d")

conn = snowflake.connector.connect(
    user='****************',
    password='****************',
    account='****************',
    warehouse='****************'
)
cur = conn.cursor()


##  HB
results_list_hb = cur.execute("SELECT * FROM edwprodhh.edi_837_parser.export_data_dimensions where hbpb in ('HB', 'BOTH')").fetchall()
results_text_hb = "\n".join(t[0] for t in results_list_hb)

target_file_name_hb = f"C:/Users/jchang/Desktop/Projects/edi-837-parser/837I/data/out/export-837I-hb-{today_str}.837"

with open(target_file_name_hb, "w", encoding="utf-8") as f:
    f.write(results_text_hb)


##  PB
results_list_pb = cur.execute("SELECT * FROM edwprodhh.edi_837_parser.export_data_dimensions where hbpb in ('PB', 'BOTH')").fetchall()
results_text_pb = "\n".join(t[0] for t in results_list_pb)

target_file_name_pb = f"C:/Users/jchang/Desktop/Projects/edi-837-parser/837I/data/out/export-837I-pb-{today_str}.837"

with open(target_file_name_pb, "w", encoding="utf-8") as f:
    f.write(results_text_pb)


cur.close()
conn.close()