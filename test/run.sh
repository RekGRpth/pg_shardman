./dtmbench  \
-c "dbname=postgres host=localhost port=5433 sslmode=disable" \
-c "dbname=postgres host=localhost port=5434 sslmode=disable" \
-c "dbname=postgres host=localhost port=5435 sslmode=disable" \
-n 10000 -a 100000 -w 10 -r 0 $*
