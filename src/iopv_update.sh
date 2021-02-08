date
python3 iopvfire.py -a iopv_data.txt
python3 stockcharts.py -f -L stocklist.txt
gzip -c iopv_data.txt > iopv_data.txt.gz
python3 firestorage.py upload charts/etf_charts.html etf_charts.html
python3 firestorage.py upload archive/iopv_data.txt.gz iopv_data.txt.gz
