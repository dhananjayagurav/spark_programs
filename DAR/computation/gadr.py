import pandas as pd
import csv
import MySQLdb
import os
import sys

input_csv_path = sys.argv[1]
d_hdfs_path = sys.argv[2]
d_hdfs_file = sys.argv[3]
output_csv_path = sys.argv[4]

def csv_cc():
    db = MySQLdb.connect("server_ip", "user", "passwd", "db")
    cursor = db.cursor()
    sql = "select dId, aId, pId, dpId from tab1, (select SUBSTRING_INDEX(expression,':',1) as dp, SUBSTRING_INDEX(expression,':',-1) as ps, dId from (select distinct se,dId from tab2 where se is not null) a) b where tab1.dp = b.dp and tab1.aId = b.ps"
    cursor.execute(sql)
    rows = cursor.fetchall()
    c = csv.writer(open(input_csv_path,"wb"))
    for r in rows:
        c.writerow(r)
    db.close()


os.system("hdfs dfs -getmerge "+d_hdfs_path+" "+d_hdfs_file)
d_r_data = pd.read_csv(d_hdfs_file,header=None,names=["pId","sId","d1Ib","se","cId","metric1","metric2","metric3"])
d_r_data["dId"],d_r_data["pId"] = zip(*d_r_data["se"].map(lambda x: x.split(':')))
print d_r_data

csv_cc()
d_db_data = pd.read_csv(input_csv_path,header=None,names=["d2Id","aId","pId","dpId"])
print d_db_data

joined_data = pd.merge(d_r_data,d_db_data, how='inner', left_on="d1Ib", right_on="d2Ib")
print joined_data
exit(0)

keep_col = ["pId","sId","dId","aId","metric1","metric2"]
joined_data1 = joined_data[keep_col]
joined_data2 = joined_data1.groupby("pId","sId","dId","aId")
joined_data2.to_csv(output_csv_path, index=False)