package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"github.com/tealeg/xlsx"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var CutQueueName string = "price_lists.cut"
var PushQueueName string = "price_lists.push"
var AMQPExchange string = "amq.direct"

type ColumnsConfig struct {
	OwnerId     int `json:"owner_id"`
	Code        int `json:"code"`
	ReplaceCode int `json:"replace_code"`
	Brand       int `json:"brand"`
	Description int `json:"description"`
	Price       int `json:"price"`
	Amount      int `json:"amount"`
	Comment     int `json:"comment"`
}

type Upload struct {
	ID            int64           `json:"id"`
	PriceListId   int64           `json:"price_list_id"`
	Status        int             `json:"status"`
	FileBasename  string          `json:"file_basename"`
	FileName      string          `json:"file_name"`
	FilePath      string          `json:"file_path"`
	FileSize      float64         `json:"file_size"`
	Brand         string          `json:"brand"`
	Currency      string          `json:"currency"`
	CurrencyValue float64         `json:"currency_value"`
	Markup        float64         `json:"markup"`
	ColDelimiter  string          `json:"col_delimiter"`
	CharacterSet  string          `json:"character_set"`
	CommentPrice  string          `json:"comment_price"`
	ColumnsConfig json.RawMessage `json:"columns_config"`
	StartRow      int             `json:"start_row"`
	Rows          int             `json:"rows"`
	RowsLoaded    int             `json:"rows_loaded"`
	RowsError     int             `json:"rows_error"`
	LoadedAt      string          `json:"loaded_at"`
	CreatedBy     int64           `json:"created_by"`
}

type Nomenclature struct {
	ID          int64  `json:"id"`
	Code        string `json:"code"`
	ReplaceCode string `json:"replace_code"`
	Brand       string `json:"brand"`
	Description string `json:"description"`
}

type UploadQueue struct {
	UploadId int `json:"upload_id"`
}

type outputer func(s string)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type xlsxMissRow struct {
	Idx int
	Row *xlsx.Row
}

/**
reed from queue response upload id and reed upload config from DB
 */
func getFileData(queueBody []byte, db *sql.DB) (Upload) {
	var jsonQueue UploadQueue
	err := json.Unmarshal(queueBody, &jsonQueue)
	failOnError(err, "Unknown JSON format")
	var upload Upload

	err = db.QueryRow("SELECT id, price_list_id, status, file_basename, file_name, file_path, file_size, ifnull(brand, '') as brand, "+
		" currency, currency_value, markup, col_delimiter, character_set, ifnull(comment_price, '') as comment_price, "+
		" columns_config, start_row, rows, rows_loaded, rows_error, ifnull(loaded_at, '') as loaded_at, "+
		" created_by FROM price_list_uploads where id = ?", jsonQueue.UploadId).Scan(&upload.ID,
		&upload.PriceListId, &upload.Status, &upload.FileBasename, &upload.FileName, &upload.FilePath, &upload.FileSize,
		&upload.Brand, &upload.Currency, &upload.CurrencyValue, &upload.Markup, &upload.ColDelimiter, &upload.CharacterSet,
		&upload.CommentPrice, &upload.ColumnsConfig, &upload.StartRow, &upload.Rows, &upload.RowsLoaded, &upload.RowsError,
		&upload.LoadedAt, &upload.CreatedBy)

	if err != nil {
		failOnError(err, "Failed: "+err.Error())
	}

	return upload
}

func getNomenclature(code string, brand string, db *sql.DB) (Nomenclature, error) {
	var nomenclature Nomenclature

	err := db.QueryRow("SELECT id, code, replace_code, brand, description FROM nomenclatures "+
		"where code = ? and brand = ?", code, brand).Scan(&nomenclature.ID, &nomenclature.Code, &nomenclature.ReplaceCode,
		&nomenclature.Brand, &nomenclature.Description)

	if err != sql.ErrNoRows {
		return nomenclature, nil
	}

	return nomenclature, err
}

func addNomenclature(nomenclature Nomenclature, userBy int64) (string) {

	return fmt.Sprintf("(\"%s\", \"%s\", \"%s\", \"%s\", %d, %d, %d, NOW(), NOW())",
		nomenclature.Code, nomenclature.ReplaceCode, nomenclature.Brand, nomenclature.Description, 1, userBy, userBy)
}

func insertNomenclatures(nomenclatures []string, db *sql.DB) {
	log.Println("Add missed nomenclatures: " + strconv.FormatInt(int64(len(nomenclatures)), 10))
	log.Println(nomenclatures)
	if nomenclatures != nil {
		db.Exec("SET autocommit=0;")
		db.Exec("SET unique_checks=0;")
		db.Exec("SET foreign_key_checks=0;")
		var bulkCount int = 50000
		var bulkNomenclatures []string
		pages := int(math.Ceil(float64(len(nomenclatures)/bulkCount) + 0.5))
		lenNomenclatures := len(nomenclatures)
		for i := 1; i <= pages; i++ {
			start := (i - 1) * bulkCount
			end := (i * bulkCount) - 1
			if end > lenNomenclatures-1 {
				end = lenNomenclatures - 1
			}

			bulkNomenclatures = nomenclatures[start:end]
			insert, err := db.Query("INSERT INTO nomenclatures " +
				" (code, replace_code, brand, description, is_auto_added, created_by, updated_by, created_at, updated_at) VALUES " +
				strings.Join(bulkNomenclatures, ", "))
			if err == nil {
				insert.Close()
			}
			if err != nil {
				failOnError(err, "Failed: "+err.Error())
			}
		}
		db.Exec("SET autocommit=1;")
		db.Exec("SET unique_checks=1;")
		db.Exec("SET foreign_key_checks=1;")
		log.Println("Miss nomenclatures add")
	}
}

func insertPrices(prices []string, db *sql.DB) {
	log.Println("Prices count: " + strconv.FormatInt(int64(len(prices)), 10))
	if prices != nil {
		var bulkCount int = 10000
		var bulkPrices []string

		db.Exec("SET autocommit=0;")
		db.Exec("SET unique_checks=0;")
		db.Exec("SET foreign_key_checks=0;")
		pages := int(math.Ceil(float64(len(prices)/bulkCount) + 0.5))
		lenPrices := len(prices)
		for i := 1; i <= pages; i++ {
			start := (i - 1) * bulkCount
			end := (i * bulkCount) - 1
			if end > lenPrices-1 {
				end = lenPrices - 1
			}

			bulkPrices = prices[start:end]
			insert, err := db.Query("INSERT INTO prices " +
				" (nomenclature_id, price_list_id, upload_id, owner_id, code, replace_code, " +
				" brand, description price, price_default, amount, comment, created_by, created_at) VALUES " +
				strings.Join(bulkPrices, ", "))
			if err == nil {
				insert.Close()
			}
			if err != nil {
				failOnError(err, "Failed: "+err.Error())
			}
		}
		db.Exec("SET autocommit=1;")
		db.Exec("SET unique_checks=1;")
		db.Exec("SET foreign_key_checks=1;")
		log.Println("Prices add")
	}
}

func addErr(uploadId int64, row int, textError string, db *sql.DB) {
	db.Exec("SET autocommit=0;")
	db.Exec("SET unique_checks=0;")
	db.Exec("SET foreign_key_checks=0;")
	insert, err := db.Query("INSERT INTO price_list_upload_rows "+
		" (upload_id, no_row, text, created_at) VALUES (?, ?, ?, NOW())", uploadId, row, textError)
	db.Exec("SET autocommit=1;")
	db.Exec("SET unique_checks=1;")
	db.Exec("SET foreign_key_checks=1;")
	if err == nil {
		insert.Close()
	}
}

func reedFile(fileConfig Upload, db *sql.DB, ch *amqp.Channel) {
	fmt.Println("Start read file: " + fileConfig.FileName)
	var columnsConfig ColumnsConfig
	err := json.Unmarshal(fileConfig.ColumnsConfig, &columnsConfig)
	if err != nil {
		failOnError(err, "Failed column config: "+err.Error())
	}

	var newNomenclatures []string

	ext := filepath.Ext(fileConfig.FileName)
	switch ext {
	case ".xls":
		fmt.Println("XLS")
	case ".xlsx":
		fmt.Println("XLSX")
		xlFile, err := xlsx.OpenFile(fileConfig.FileName)
		failOnError(err, "Failed open file: "+fileConfig.FileName)

		sheet := xlFile.Sheets[0]
		var missRows []xlsxMissRow
		var priceRows []string
		log.Println("File opened. Rows: " + strconv.FormatInt(int64(sheet.MaxRow), 10)) //todo update rows count
		log.Println("Start pars")
		for idx, row := range sheet.Rows {
			if row != nil && idx >= fileConfig.StartRow-1 {
				priceRow, missRow, newNomenclature := searchNomenclatures(idx, row, fileConfig, columnsConfig, db)
				if priceRow != "" {
					priceRows = append(priceRows, priceRow)
				}

				if missRow != nil {
					missRows = append(missRows, xlsxMissRow{Idx: idx, Row: missRow})
				}
				if newNomenclature != "" {
					newNomenclatures = append(newNomenclatures, newNomenclature)
				}
			}
		}
		log.Println("End pars")
		insertNomenclatures(newNomenclatures, db)
		log.Println("Research each new nomenclature")
		for _, row := range missRows {
			priceRow, _, _ := searchNomenclatures(row.Idx, row.Row, fileConfig, columnsConfig, db)
			if priceRow != "" {
				priceRows = append(priceRows, priceRow)
			}
		}
		log.Println("Inserting prices")
		insertPrices(priceRows, db)
		log.Println("Done")

	case ".csv":
		fmt.Println("CSV")
	case ".txt":
		fmt.Println("TXT")
	default:
		log.Fatalln("Unknown format: " + ext)

	}
}

func searchNomenclatures(idx int, row *xlsx.Row, fileConfig Upload, columnsConfig ColumnsConfig, db *sql.DB) (string, *xlsx.Row, string) {
	cells := row.Cells
	uploadId := fileConfig.ID
	priceListId := fileConfig.PriceListId
	var err error
	var ownerId string
	var nomenclature Nomenclature
	var brand string
	var code string
	var replaceCode string = ""
	var description string = ""
	var price float64
	var priceDefault float64
	var amount int64
	var comment string = ""

	if fileConfig.Brand != "" {
		brand = string(fileConfig.Brand)
	} else {
		brand, err = cells[columnsConfig.Brand-1].FormattedValue()
		if err != nil {
			addErr(fileConfig.ID, idx+1, "Не верный формат бренда", db)
			return "", nil, ""
		}
	}
	code, err = cells[columnsConfig.Code-1].FormattedValue()
	if err != nil {
		addErr(fileConfig.ID, idx+1, "Не верный формат кода номенклатуры", db)
		return "", nil, ""
	}

	if code == "" || brand == "" {
		addErr(fileConfig.ID, idx+1, "Не верые данные номенклатуры", db)
		return "", nil, ""
	}

	var nomenclatureErr error

	nomenclature, nomenclatureErr = getNomenclature(code, brand, db)

	if columnsConfig.ReplaceCode > 0 {
		replaceCode, err = cells[columnsConfig.ReplaceCode-1].FormattedValue()
		if err != nil {
			addErr(fileConfig.ID, idx+1, "Не верный формат кода заменителя", db)
			return "", nil, ""
		}
	}

	if columnsConfig.Description > 0 {
		description, err = cells[columnsConfig.Description-1].FormattedValue()
		if err != nil {
			addErr(fileConfig.ID, idx+1, "Не верный формат описания позиции", db)
			return "", nil, ""
		}
	}
	if nomenclatureErr != nil {
		nomenclature.Code = code
		nomenclature.Brand = brand
		nomenclature.ReplaceCode = replaceCode
		nomenclature.Description = description

		return "", row, addNomenclature(nomenclature, fileConfig.CreatedBy)
	}

	priceDefaultCellValue, err := cells[columnsConfig.Price-1].FormattedValue()
	if err != nil {
		addErr(fileConfig.ID, idx+1, "Не верный формат цены", db)
		return "", nil, ""
	}

	priceDefault, err = strconv.ParseFloat(priceDefaultCellValue, 64)
	if err != nil {
		addErr(fileConfig.ID, idx+1, "Не определена цена", db)
		return "", nil, ""
	}

	price = priceDefault * fileConfig.CurrencyValue * fileConfig.Markup

	amountCellValue, err := cells[columnsConfig.Amount-1].FormattedValue()
	if err != nil {
		addErr(fileConfig.ID, idx+1, "Не верный формат количества", db)
		return "", nil, ""
	}
	amount, err = strconv.ParseInt(amountCellValue, 10, 64)
	if err != nil {
		addErr(fileConfig.ID, idx+1, "Не определено количество", db)
		return "", nil, ""
	}

	ownerId, err = cells[columnsConfig.OwnerId-1].FormattedValue()
	if err != nil {
		addErr(fileConfig.ID, idx+1, "Не верный формат идентификатора владельца позиции", db)
		return "", nil, ""
	}
	if ownerId == "" {
		addErr(fileConfig.ID, idx+1, "Не определен идентификатор владельца позиции", db)
		return "", nil, ""
	}

	if columnsConfig.Comment > 0 {
		comment, err = cells[columnsConfig.Comment-1].FormattedValue()
		if err != nil {
			addErr(fileConfig.ID, idx+1, "Не верный формат коментария к позиции", db)
			return "", nil, ""
		}
	}

	return fmt.Sprintf("(%d,%d,%d,\"%s\",\"%s\",\"%s\",\"%s\",%f,%f,%d,\"%s\",%d,%s)",
		nomenclature.ID, priceListId, uploadId, ownerId, nomenclature.Code, nomenclature.ReplaceCode,
		nomenclature.Description, price, priceDefault, amount, comment, fileConfig.CreatedBy, "NOW()"), nil, ""
}

func initAMQP() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		CutQueueName, // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		AMQPExchange,
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	/*q, err = ch.QueueDeclare(
		PushQueueName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,
		"",
		AMQPExchange,
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")*/

	return conn, ch
}

func main() {
	godotenv.Load()

	conn, ch := initAMQP()
	defer conn.Close()
	defer ch.Close()

	db, err := sql.Open("mysql", os.Getenv("MYSQL_URL"))
	failOnError(err, "Failed to connect to MySql")
	defer db.Close()

	msgs, err := ch.Consume(
		CutQueueName, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)

			reedFile(getFileData(d.Body, db), db, ch)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}
