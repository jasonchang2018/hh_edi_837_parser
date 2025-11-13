import snowflake.connector
from datetime import datetime
import pandas as pd
import re

today_str = datetime.now().strftime("%Y%m%d")


##  Read Snowflake Password
with open("C:/Users/jchang/Desktop/Projects/edi-837-parser/837I/src/py/secrets.txt") as f:
    password = f.read().strip()

conn = snowflake.connector.connect(
    user='JCHANG',
    password=password,
    account='OITBPKZ-WPA42783',
    warehouse='ANALYSIS_WH'
)
cur = conn.cursor()


##  Return Snowflake Results
results_list = cur.execute("SELECT * FROM edwprodhh.edi_837i_parser.export_data_dimensions").fetchall()

##  Convert to DF
df = pd.DataFrame(results_list, columns = [desc[0] for desc in cur.description])


##  For each PL Group, Convert to Text and Export File
for value, group_df in df.groupby("PL_GROUP"):

    # Convert the relevant column(s) to your output text format.
    # If you want entire rows, adjust this part.

    results_text = ("\n".join(
        group_df
            .sort_values(by = group_df.columns[1])
            .iloc[:, 0]
            .astype(str)
    ))

    # results_text = "\n".join(group_df.iloc[:, 0].astype(str).tolist())  
    # results_text_hb = "\n".join(t[0] for t in results_list)

    # Build filename — include the distinct value
    filename = (
        f"C:/Users/jchang/Desktop/Projects/edi-837-parser/837I/data/out/"
        f"export-837I-HB-{re.sub(r"[^\w]", "", value)}-{today_str}.837"
    )

    with open(filename, "w") as f:
        f.write(results_text)

    print(f"Created file: {filename}")


cur.close()
conn.close()