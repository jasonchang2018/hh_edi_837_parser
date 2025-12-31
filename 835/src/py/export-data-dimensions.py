import snowflake.connector
from datetime import datetime
import pandas as pd
import re

today_str = datetime.now().strftime("%Y%m%d")


##  Read Snowflake Password
with open("C:/Users/jchang/Desktop/Projects/x12_edi_parser/835/src/py/secrets.txt") as f:
    password = f.read().strip()

conn = snowflake.connector.connect(
    user='JCHANG',
    password=password,
    account='OITBPKZ-WPA42783',
    warehouse='ANALYSIS_WH'
)
cur = conn.cursor()


##  Return Snowflake Results
results_list = cur.execute("select line_element_835, index from edwprodhh.edi_835_parser.export_splicer").fetchall()


if results_list:
    
    # Convert to DataFrame
    df = pd.DataFrame(
        results_list,
        columns=[desc[0] for desc in cur.description]
    )

    # Sort by index and concatenate line_element_835
    results_text = "\n".join(
        df
        .sort_values(by="INDEX")
        ["LINE_ELEMENT_835"]
        .astype(str)
    )

    filename = (
        f"J:/IU_Health_Complex/835_SPLICER/"
        f"export-835-{today_str}.835"
    )

    with open(filename, "w", newline="\n") as f:
        f.write(results_text)

    print(f"Created file: {filename}")


cur.close()
conn.close()