date
gzip -c iopvfire.log > iopvfire.log.gz
python3 firestorage.py upload logs/iopvfire.log.gz iopvfire.log.gz
