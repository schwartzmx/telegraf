package redshift

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"

	// postgresql driver initialization
	_ "github.com/lib/pq"
)

// Redshift struct
type Redshift struct {
	Address         string
	ClusterName     string
	IntervalSeconds int
}

// Query struct
type Query struct {
	Script         string
	OrderedColumns []string
}

// MapQuery type
type MapQuery map[string]Query

var queries MapQuery

var sampleConfig = `
  ## Specify a Redshift cluster to monitor with an address, or connection string.
  ## cluster_name is the optional name of the Redshift cluster
  ## interval_seconds is used for querying windows of metrics
  # address = "dbname='<db>' port='<p>' user='<user>' password='<pw>' host='<cluster>.<region>.redshift.amazonaws.com'"
  # cluster_name = "lucid"
  # interval_seconds = 500
`

// SampleConfig return the sample configuration
func (r *Redshift) SampleConfig() string {
	return sampleConfig
}

// Description return plugin description
func (r *Redshift) Description() string {
	return "Read metrics from Amazon Redshift"
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func initQueries(r *Redshift) {
	queries = make(MapQuery)
	queries["ColumnsNotCompressed"] = Query{Script: rsColumnsNotCompressed}
	queries["TableInfo"] = Query{Script: rsTableInfo}
	queries["QueryScanNoSort"] = Query{Script: queryFmt(rsQueryScanNoSort, r.IntervalSeconds)}
}

func queryFmt(query string, interval int) string {
	return fmt.Sprintf(query, interval)
}

// Gather collect data from Redshift
func (r *Redshift) Gather(acc telegraf.Accumulator) error {
	initQueries(r)

	var wg sync.WaitGroup
	var outerr error

	for _, query := range queries {
		wg.Add(1)
		go func(addr string, query Query) {
			defer wg.Done()
			outerr = r.gather(addr, query, acc)
		}(r.Address, query)
	}

	wg.Wait()
	return outerr
}

func (r *Redshift) gather(addr string, query Query, acc telegraf.Accumulator) error {
	// deferred opening
	conn, err := sql.Open("postgres", addr)
	if err != nil {
		return err
	}
	// verify that a connection can be made before making a query
	err = conn.Ping()
	if err != nil {
		// Handle error
		return err
	}
	defer conn.Close()

	// execute query
	rows, err := conn.Query(query.Script)
	if err != nil {
		return err
	}
	defer rows.Close()

	// grab the column information from the result
	query.OrderedColumns, err = rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		err = r.accRow(query, acc, rows)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func (r *Redshift) accRow(query Query, acc telegraf.Accumulator, row scanner) error {
	var columnVars []interface{}
	var fields = make(map[string]interface{})

	// store the column name with its *interface{}
	columnMap := make(map[string]*interface{})
	for _, column := range query.OrderedColumns {
		columnMap[column] = new(interface{})
	}
	// populate the array of interface{} with the pointers in the right order
	for i := 0; i < len(columnMap); i++ {
		columnVars = append(columnVars, columnMap[query.OrderedColumns[i]])
	}
	// deconstruct array of variables and send to Scan
	err := row.Scan(columnVars...)
	if err != nil {
		return err
	}

	tags := map[string]string{"cluster": r.ClusterName}

	for col, val := range columnMap {
		fields[col] = *val
	}
	acc.AddFields("redshift", fields, tags)
	return nil
}

func init() {
	inputs.Add("redshift", func() telegraf.Input {
		return &Redshift{}
	})
}

// queries
var rsColumnsNotCompressed = `
SELECT
    count(a.attname) as "Columns Not Compressed"
FROM pg_namespace n, pg_class c, pg_attribute a  
WHERE n.oid = c.relnamespace 
AND c.oid = a.attrelid 
AND a.attnum > 0 
AND NOT a.attisdropped 
and n.nspname NOT IN ('information_schema','pg_catalog','pg_toast') 
AND format_encoding(a.attencodingtype::integer) = 'none' 
AND c.relkind='r' AND a.attsortkeyord != 1;
`

var rsTableInfo = `
select
	"schema" || '.' || "table" as tablename, encoded, max_varchar, unsorted, stats_off, tbl_rows, skew_sortkey1, skew_rows 
into temp table tableinfo
from svv_table_info;

select
	(
		select sum(case when encoded = 'N' then 1 else 0 end)
		from tableinfo
	) as "Tables Not Compressed",
	(
		select max(case when isnull(skew_rows,0) >= isnull(skew_sortkey1,0)
				then isnull(skew_rows,0)
				else isnull(skew_sortkey1,0)
				end
				)
		from tableinfo
	) as "Max Skew Sort Ratio" ,
	(
		select sum(isnull(skew_rows,0)) + sum(isnull(skew_sortkey1,0))
		from tableinfo
	) as "Total Skew Sort Ratio" ,
	(
		select sum(case when skew_rows is not null then 1 else 0 end) + sum(case when skew_sortkey1 is not null then 1 else 0 end)
		from tableinfo
	) as "Number of Tables Skew Sort" ,
	(
		select sum(case when isnull(skew_rows, 0) > 0 then 1 else 0 end)
		from tableinfo
	) as "Number of Tables Skewed",
	(
		select sum(case when stats_off is not null then 1 else 0 end)
		from tableinfo
	) as "Number of Tables Stats Off",
	(
		select max(isnull(max_varchar,0))
		from tableinfo
	) as "Max VarChar Size",
	(
		select max(isnull(unsorted,0))
		from tableinfo
	) as "Max Unsorted Percent",
	(
		select sum(isnull(tbl_rows,0))
		from tableinfo
	) as "Total Table Rows";

`

var rsQueryScanNoSort = `
SELECT sum(nvl(s.num_qs,0)) as "Query Scans No Sort"
FROM svv_table_info t 
LEFT JOIN (
	SELECT tbl, COUNT(distinct query) num_qs 
	FROM stl_scan s 
	WHERE s.userid > 1 AND starttime >= GETDATE() - INTERVAL '%d seconds' 
	GROUP BY tbl) s 
ON s.tbl = t.table_id WHERE t.sortkey1 IS NULL
`
