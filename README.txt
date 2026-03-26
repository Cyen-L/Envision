== Re-initialization ==
docker-compose down -v
Remove-Item -Recurse -Force clickhouse-data
docker-compose up -d
python ./main.py

C1 - Daily total transaction count (query output)
SELECT toDate(transaction_time) AS day, COUNT(*) AS total_count FROM olap_db.transactions GROUP BY day ORDER BY day;

C2 - Daily total transfer amount (query output)
SELECT toDate(transaction_time) AS day, SUM(transfer_amount) AS total_amount FROM olap_db.transactions GROUP BY day ORDER BY day;

C3 - Daily count by currency (query output)
SELECT toDate(transaction_time) AS day, currency_code, COUNT(*) AS total_count FROM olap_db.transactions GROUP BY day, currency_code ORDER BY day, currency_code;