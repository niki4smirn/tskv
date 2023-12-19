#! /bin/bash


dbs=("clickhouse" "influx" "victoriametrics")

for db in "${dbs[@]}"
do
    echo "Generating data for $db"
    if [ $db == "clickhouse" ]
    then
        cd clickhouse-insert
        go run main.go
        cd -
    else
        tsbs_generate_data --use-case="devops" --seed=123 --scale=8 \
                        --timestamp-start="2016-01-01T00:00:00Z" \
                        --timestamp-end="2016-01-02T00:00:00Z" \
                        --log-interval="1s" --format=$db \
                        | gzip > /tmp/$db-data.gz

        cd $GOPATH/pkg/mod/github.com/timescale/tsbs@v0.0.0-20230921131859-37fced794d56
        NUM_WORKERS=1 BATCH_SIZE=15000 BULK_DATA_DIR=/tmp scripts/load/load_$db.sh
        cd -
    fi
done

query_modes=("1-1-1" "1-1-12" "1-8-1" "5-1-1" "5-1-12" "5-8-1")

for db in "${dbs[@]}"
do
    for query_mode in "${query_modes[@]}"
    do
        echo "Generating queries for $db with query mode $query_mode"
        tsbs_generate_queries --use-case="devops" --seed=123 --scale=8 \
            --timestamp-start="2016-01-01T00:00:00Z" \
            --timestamp-end="2016-01-02T00:00:01Z" \
            --queries=10000 --query-type="single-groupby-"$query_mode --format=$db \
            | gzip > /tmp/$db"-queries-single-groupby-"$query_mode.gz

        cat /tmp/$db-queries-single-groupby-$query_mode.gz | \
                    gunzip | tsbs_run_queries_$db --workers=1 --print-interval=0
    done
done

