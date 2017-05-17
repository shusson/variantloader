package main

import (
	"fmt"
	"flag"
	"os"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	var dsn string
	var file string
	var dbName string
	var tName string
	var clean bool
	flag.StringVar(&dsn, "d", "root:root@tcp(127.0.0.1:3306)/", "mysql dsn: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]")
	flag.StringVar(&file, "f", "/data/test.tsv", "path to tsv file to be loaded from the perspective of the sql server")
	flag.StringVar(&dbName, "db", "v", "name of database to create")
	flag.StringVar(&tName, "t", "vs", "name of table to create and load into")
	flag.BoolVar(&clean, "c", false, "if true will drop existing db and table")

	flag.Usage = func() {
		fmt.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	db, err := sql.Open("mysql", dsn)
	check(err)
	defer db.Close()
	err = db.Ping()
	check(err)

	if clean {
		dropDatabase(dbName, db)
	}
	createDatabase(dbName, db)
	createTable(tName, db)
	loadData(file, tName, db)
}

func dropDatabase(dbName string, db *sql.DB) {
	_, err := db.Exec("DROP DATABASE " + dbName)
	check(err)
	fmt.Printf("Dropped db %s\n", dbName)
}

func createDatabase(dbName string, db *sql.DB) {
	_, err := db.Exec("CREATE DATABASE " + dbName)
	check(err)
	fmt.Printf("Created db %s\n", dbName)

	_, err = db.Exec("USE " + dbName)
	check(err)

	fmt.Printf("Used db %s\n", dbName)
}

func createTable(tName string, db *sql.DB) {

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		chromosome VARCHAR(2),
		start INT,
		reference TEXT,
		alternate TEXT,
		dbSNP TEXT,
		callRate DECIMAL(6,5),
		AC INT,
		AF DECIMAL(6,5),
		nCalled INT,
		nNotCalled INT,
		nHomRef INT,
		nHet INT,
		nHomVar INT,
		dpMean TEXT,
		dpStDev TEXT,
		gqMean TEXT,
		gqStDev TEXT,
		nNonRef INT,
		rHeterozygosity DECIMAL(6,5),
		rHetHomVar TEXT,
		rExpectedHetFrequency DECIMAL(6,5),
		pHWE FLOAT(12),
		INDEX name (chromosome, start)
	)`, tName))
	check(err)
	fmt.Printf("Created table %s\n", tName)
}

func loadData(file string, tName string, db *sql.DB) {

	_, err := db.Exec(fmt.Sprintf(`LOAD DATA INFILE "%s" INTO TABLE %s
				  FIELDS TERMINATED BY '\t'
				  LINES TERMINATED BY '\n'
				  IGNORE 1 LINES`,
		 		  file, tName))
	check(err)
}

func check(err error) {
	if err == nil {
		return
	}
	panic(err)
}
