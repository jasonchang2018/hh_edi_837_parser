import snowflake.connector

conn = snowflake.connector.connect(
    user='****************',
    password='****************',
    account='****************',
    warehouse='****************'
)
cur = conn.cursor()


results_list = cur.execute("SELECT * FROM edwprodhh.edi_837_parser.export_data_dimensions").fetchall()
results_text = "\n".join(t[0] for t in results_list)

target_file_name = "C:/Users/jchang/Downloads/test-output.837"

with open(target_file_name, "w", encoding="utf-8") as f:
    f.write(results_text)


cur.close()
conn.close()