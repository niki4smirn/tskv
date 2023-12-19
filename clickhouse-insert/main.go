package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	MB        = 1024 * 1024
	BatchSize = 1 * MB
)

func dropTable(conn driver.Conn, metricType string) error {
	err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", metricType))
	return err
}

func createTable(conn driver.Conn, metricType string, columns []string) error {
	var columnsWithTypes string
	for idx, column := range columns {
		columnsWithTypes += column + " Float64"
		if idx != len(columns)-1 {
			columnsWithTypes += ", "
		}
	}

	query := fmt.Sprintf(
		"CREATE TABLE %s (created_date Date, created_at DateTime, tags_id UInt64, %s) ENGINE = MergeTree(created_date, (tags_id, created_date), 8192);",
		metricType,
		columnsWithTypes,
	)
	err := conn.Exec(context.Background(), query)
	return err
}

const (
	FirstLine = iota
	Types     = iota
	DataInfo  = iota
	Data      = iota
)

type Tag struct {
	hostname           string
	region             string
	datacenter         string
	rack               string
	os                 string
	arch               string
	team               string
	service            string
	serviceVersion     string
	serviceEnvironment string
}

func dateTimeString(timestamp uint64) string {
	return time.Unix(0, int64(timestamp)).Format("2006-01-02 15:04:05")
}

func dateString(timestamp uint64) string {
	return time.Unix(0, int64(timestamp)).Format("2006-01-02")
}

func fillTags(conn driver.Conn, tags map[Tag]uint64) error {
	{
		err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS tags")
		if err != nil {
			return err
		}
	}
	{
		err := conn.Exec(context.Background(), "CREATE TABLE tags (created_date Date, id UInt64,hostname String, region String, datacenter String, rack String, os String, arch String, team String, service String, service_version String, service_environment String) ENGINE = MergeTree(created_date, (id), 8192)")
		if err != nil {
			return err
		}
	}
	{
		batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO tags")
		if err != nil {
			return err
		}

		for tag, id := range tags {
			err := batch.Append("2016-01-01", id, tag.hostname, tag.region, tag.datacenter, tag.rack, tag.os, tag.arch, tag.team, tag.service, tag.serviceVersion, tag.serviceEnvironment)
			if err != nil {
				return err
			}
		}

		err = batch.Send()
		if err != nil {
			return err
		}
	}
	return nil
}

func readData(conn driver.Conn) (map[string][][]any, map[string][]string) {
	file, err := os.Open("../test_data/timescaledb-data-8-1s-24h")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	metricToColumns := make(map[string][]string)
	insertsQueue := make(map[string][][]any)
	tags := make(map[Tag]uint64)
	var lastTagId uint64
	var previousTagId uint64
	state := FirstLine

	for scanner.Scan() {
		line := scanner.Text()
		if state == FirstLine {
			state = Types
			continue
		} else if state == Types {
			if line == "" {
				state = DataInfo
				continue
			}
			split := strings.Split(line, ",")
			metricType := split[0]
			columns := split[1:]
			metricToColumns[metricType] = columns
			err := dropTable(conn, metricType)
			if err != nil {
				log.Fatal(err)
			}
			err = createTable(conn, metricType, columns)
			if err != nil {
				log.Fatal(err)
			}
		} else if state == DataInfo {
			split := strings.Split(line, ",")
			var tag Tag
			tag.hostname = strings.Split(split[1], "=")[1]
			tag.region = strings.Split(split[2], "=")[1]
			tag.datacenter = strings.Split(split[3], "=")[1]
			tag.rack = strings.Split(split[4], "=")[1]
			tag.os = strings.Split(split[5], "=")[1]
			tag.arch = strings.Split(split[6], "=")[1]
			tag.team = strings.Split(split[7], "=")[1]
			tag.service = strings.Split(split[8], "=")[1]
			tag.serviceVersion = strings.Split(split[9], "=")[1]
			tag.serviceEnvironment = strings.Split(split[10], "=")[1]

			if _, ok := tags[tag]; !ok {
				lastTagId++
				tags[tag] = lastTagId
			}

			previousTagId = tags[tag]
			state = Data
		} else if state == Data {
			split := strings.Split(line, ",")
			metricType := split[0]
			timestamp, err := strconv.ParseUint(split[1], 10, 64)
			if err != nil {
				log.Fatal(err)
			}
			data := strings.Join(split[2:], ",")

			floatData := make([]float64, 0)
			for _, value := range strings.Split(data, ",") {
				floatValue, err := strconv.ParseFloat(value, 64)
				if err != nil {
					log.Fatal(err)
				}
				floatData = append(floatData, floatValue)
			}

			toAppend := make([]any, 0)
			toAppend = append(toAppend, []any{
				dateString(timestamp),
				dateTimeString(timestamp),
				previousTagId,
			}...)
			for _, value := range floatData {
				toAppend = append(toAppend, value)
			}
			insertsQueue[metricType] = append(insertsQueue[metricType], toAppend)

			state = DataInfo
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	err = fillTags(conn, tags)
	if err != nil {
		log.Fatal(err)
	}

	return insertsQueue, metricToColumns
}

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "benchmark",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal(err)
	}

	insertsQueue, metricToColumns := readData(conn)

	start := time.Now()

	for metricType, inserts := range insertsQueue {
		curBatchSize := BatchSize / (len(metricToColumns[metricType]) - 3) / 8

		for i := 0; i < len(inserts); i += curBatchSize {
			batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", metricType))
			if err != nil {
				log.Fatal(err)
			}
			for j := i; j < i+curBatchSize && j < len(inserts); j++ {
				insert := inserts[j]
				err := batch.Append(insert...)
				if err != nil {
					log.Fatal(err)
				}
			}

			err = batch.Send()
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	elapsed := time.Since(start)
	log.Printf("Inserting data took %s", elapsed)
}
