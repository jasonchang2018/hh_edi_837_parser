import snowflake.connector

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

target_file_name_hb = "C:/Users/jchang/Downloads/test-output-hb.837"

with open(target_file_name_hb, "w", encoding="utf-8") as f:
    f.write(results_text_hb)


##  PB
results_list_pb = cur.execute("SELECT * FROM edwprodhh.edi_837_parser.export_data_dimensions where hbpb in ('PB', 'BOTH')").fetchall()
results_text_pb = "\n".join(t[0] for t in results_list_pb)

target_file_name_pb = "C:/Users/jchang/Downloads/test-output-pb.837"

with open(target_file_name_pb, "w", encoding="utf-8") as f:
    f.write(results_text_pb)


cur.close()
conn.close()