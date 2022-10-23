//-*- coding: utf-8 -*-

// About: A converter from MS Access/Jet database created by
// Hogia Hemekonomi (mid 1990's) to text. Probably only useful for
// debugging of wHHEK or possibly archiving.

// System Requirements: Windows 10 (any)

// Prepare: install gnu emacs: emacs-26.3-x64_64 (optional)
// Prepare: TDM-GCC from https://jmeubank.github.io/tdm-gcc/
//https://github.com/jmeubank/tdm-gcc/releases/download/v9.2.0-tdm-1/tdm-gcc-9.2.0.exe

// Prepare: install git: Git-2.23.0-64-bit
// Prepare: install golang 32-bits (can't access access/jet driver using 64-bits)
//   go1.16.3.windows-386.msi
// Prepare: go get github.com/mattn/go-adodb
// Build: go build hhek2txt.go
// Run: ./hhek2txt.exe -help
// Run: ./hhek2txt.exe -optin=hemekonomi.mdb
// System requirements for hhek2txt.exe is Windows XP or later

package main

import (
	"encoding/hex"
	"fmt"
	"flag"
	"log"
	"os"
	"strings"
	"golang.org/x/text/encoding/charmap"
	"reflect"
	"database/sql"

	"github.com/go-ole/go-ole"
	_ "github.com/go-ole/go-ole/oleutil"
	_ "github.com/mattn/go-adodb"
)

func toUtf8(in_buf []byte) string {
	var buf []byte
	
	buf, _ = charmap.Windows1252.NewDecoder().Bytes(in_buf)

	// Escape chars for SQL
	stringVal := string(buf)
	stringVal2 := strings.ReplaceAll(stringVal, "'", "''");
	stringVal3 := strings.ReplaceAll(stringVal2, "\"", "\"\"");
	return stringVal3
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func GetTables(db *sql.DB) ([]string , map[string][]string) {
	fmt.Println("Get Tables.")

	tables := []string{"DtbVer", "Personer", "BetalKonton", "Betalningar", "Överföringar", "Konton", "LÅN", "Platser", "Budget", "Transaktioner"}
	cols := map[string][]string {
		        "DtbVer": {"VerNum", "Benämning", "Losenord" },
			"Personer": {"Namn", "Född", "Kön", "Löpnr" },
			"BetalKonton": {"Konto", "Kontonummer", "Kundnummer", "Sigillnummer", "Löpnr" },
			"Betalningar": {"FrånKonto", "TillPlats", "Typ", "Datum", "Vad", "Vem", "Belopp", "Text", "Löpnr", "Ranta", "FastAmort", "RorligAmort", "OvrUtg", "LanLopnr", "Grey" },
			"Överföringar": {"FrånKonto", "TillKonto", "Belopp", "Datum", "HurOfta", "Vad", "Vem", "Löpnr", "Kontrollnr", "TillDatum", "Rakning" },
			"Konton": {"KontoNummer", "Benämning", "Saldo", "StartSaldo", "StartManad", "Löpnr", "SaldoArsskifte", "ArsskifteManad" },
			"LÅN": {"Langivare", "EgenBeskrivn", "LanNummer", "TotLanebelopp", "StartDatum", "RegDatum", "RantJustDatum", "SlutBetDatum", "AktLaneskuld", "RorligDel", "FastDel", "FastRanta", "RorligRanta", "HurOfta", "Ranta", "FastAmort", "RorligAmort", "OvrUtg", "Löpnr", "Rakning", "Vem", "FrånKonto", "Grey", "Anteckningar", "BudgetRanta", "BudgetAmort", "BudgetOvriga" },
			"Platser": {"Namn", "Gironummer", "Typ", "RefKonto", "Löpnr" },
			"Budget": {"Typ", "Inkomst", "HurOfta", "StartMånad", "Jan", "Feb", "Mar", "Apr", "Maj", "Jun", "Jul", "Aug", "Sep", "Okt", "Nov", "Dec", "Kontrollnr", "Löpnr" },
			"Transaktioner": {"FrånKonto", "TillKonto", "Typ", "Datum", "Vad", "Vem", "Belopp", "Löpnr", "Saldo", "Fastöverföring", "Text" }}

	return tables,cols
}

func DumpTable(db *sql.DB, tablename string, col_names []string) {
	fmt.Println("Dump Table:", tablename)

	nummerTabeller := map[string]bool {
         		"Personer": true,
			"BetalKonton": true,
			"Betalningar": true,
			"Överföringar": true,
			"Konton": true,
			"LÅN": true,
			"Platser": true,
			"Budget": true,
			"Transaktioner": true}

	sqlStmt:="SELECT "
	for pos, value := range col_names {
		if pos == 0 {
			sqlStmt+=value
		} else {
			sqlStmt+=","+value
		}
	}
	sqlStmt+=" FROM  "
	sqlStmt+=tablename

	if nummerTabeller[tablename] {
		sqlStmt+=" ORDER BY Löpnr"
	}
	fmt.Println("EXEC: ", sqlStmt)

	res, err := db.Query(sqlStmt)
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}
	defer res.Close()

	var myMap = make(map[string]interface{})
	if err != nil {
		log.Fatal(err)
	}
	colNames, err := res.Columns()
	if err != nil {
		log.Fatal(err)
	}
	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	columns, err := res.ColumnTypes()
	if err != nil {
		log.Fatal(err)
		os.Exit(2)
	}
	for _, value := range columns {
		decprecision, decscale, decok := value.DecimalSize()
		length, lenok := value.Length()
		nullable, nullok := value.Nullable()
		
		fmt.Println("COLNAME: ", value.Name(),
			"DBTYP: ", value.DatabaseTypeName() ,
			"DEC(precision, scale, ok): ", decprecision, decscale, decok,
			"LEN(len, ok): ", length, lenok,
			"NULLABLE: ", nullable, nullok, 
			"ScanType: ", value.ScanType().Name())
	}

	for res.Next() {
		err = res.Scan(colPtrs...)
		if err != nil {
			log.Fatal(err)
		}
		for i, col := range cols {
			myMap[colNames[i]] = col
		}

		//for key, val := range myMap {
		for _, key := range col_names {
			val := myMap[key]
			if val == nil {
				fmt.Println("Key:", key, "Value : NULL")
			} else {
				if str, ok := val.(float64); ok {
					fmt.Println("Key:", key, "Value f64:", str)
				} else if str, ok := val.([]uint8); ok {
					fmt.Println("Key:", key, "Value uint8string: '"+toUtf8(str)+"' "+hex.Dump(str))
				} else if str, ok := val.(int32); ok {
					fmt.Println("Key:", key, "Value int32:", str)
				} else if str, ok := val.(int64); ok {
					fmt.Println("Key:", key, "Value int64:", str)
				} else if str, ok := val.(bool); ok {
					fmt.Println("Key:", key, "Value BOOL:", str)
				} else if str, ok := val.(string); ok {
					fmt.Println("Key:", key, "Value String: '"+str+"' "+hex.Dump([]byte(str)))
				} else {
					fmt.Println("Key:", key, "Value Unhandled Type:", reflect.TypeOf(val))
				}
			}

		}
		fmt.Println(" ")
	}
}

func main() {
	optinPtr := flag.String("optin", "", "Hogia Hemekonomi database filename (*.mdb)")
	
	flag.Parse()
	
	if *optinPtr == "" {
		flag.Usage()
		os.Exit(1)
	}
	
	filename := *optinPtr;
	if !fileExists(filename) {
		fmt.Println(*optinPtr, " file does not exist (or is a directory)")
		flag.Usage()
		os.Exit(1)
	}

	ole.CoInitialize(0)
	defer ole.CoUninitialize()
	
	var err error
	var db *sql.DB
	
	provider := "Microsoft.Jet.OLEDB.4.0"
	db, err = sql.Open("adodb", "Provider="+provider+";Data Source="+filename+";")
	if err != nil {
		fmt.Println("open", err)
		return
	}
	defer db.Close()
	
	// GET TABLES
	tables, cols := GetTables(db)
	
	// DUMP TABLE
	for _, value := range tables {
		//fmt.Println(value)
		DumpTable(db, value, cols[value])
	}
	
	db.Close()
}
