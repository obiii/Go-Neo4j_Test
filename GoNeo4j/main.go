package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	driver "github.com/johnnadratowski/golang-neo4j-bolt-driver"
)

type Transaction []struct {
	BlockHash        string `json:"blockHash"`
	BlockNumber      string `json:"blockNumber"`
	From             string `json:"from"`
	Gas              string `json:"gas"`
	GasPrice         string `json:"gasPrice"`
	Hash             string `json:"hash"`
	Input            string `json:"input"`
	Nonce            string `json:"nonce"`
	To               string `json:"to"`
	TransactionIndex string `json:"transactionIndex"`
	Value            string `json:"value"`
	V                string `json:"v"`
	R                string `json:"r"`
	S                string `json:"s"`
}

type Uncles []string

type blockData struct {
	Jsonrpc string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		Difficulty      string `json:"difficulty"`
		ExtraData       string `json:"extraData"`
		GasLimit        string `json:"gasLimit"`
		GasUsed         string `json:"gasUsed"`
		Hash            string `json:"hash"`
		LogsBloom       string `json:"logsBloom"`
		Miner           string `json:"miner"`
		MixHash         string `json:"mixHash"`
		Nonce           string `json:"nonce"`
		Number          string `json:"number"`
		ParentHash      string `json:"parentHash"`
		ReceiptsRoot    string `json:"receiptsRoot"`
		Sha3Uncles      string `json:"sha3Uncles"`
		Size            string `json:"size"`
		StateRoot       string `json:"stateRoot"`
		Timestamp       string `json:"timestamp"`
		TotalDifficulty string `json:"totalDifficulty"`
		Transactions    []struct {
			BlockHash        string `json:"blockHash"`
			BlockNumber      string `json:"blockNumber"`
			From             string `json:"from"`
			Gas              string `json:"gas"`
			GasPrice         string `json:"gasPrice"`
			Hash             string `json:"hash"`
			Input            string `json:"input"`
			Nonce            string `json:"nonce"`
			To               string `json:"to"`
			TransactionIndex string `json:"transactionIndex"`
			Value            string `json:"value"`
			V                string `json:"v"`
			R                string `json:"r"`
			S                string `json:"s"`
		} `json:"transactions"`
		TransactionsRoot string        `json:"transactionsRoot"`
		Uncles           []interface{} `json:"uncles"`
	} `json:"result"`
}

var (
	neo4jURL = "bolt://neo4j:root@localhost:7687"
	client   = http.Client{}
	url      = ""
	db, err  = driver.NewDriver().OpenNeo(neo4jURL)
)

func getClient() http.Client {
	return client
}

func convertToHex(blockNumber int) string {
	hexBytes := fmt.Sprintf("%0x", blockNumber)
	hexString := fmt.Sprintf("%s", hexBytes)
	hexString = "0x" + hexString
	return hexString
}

func getBlockData(blockNumber int) []byte {
	var body []byte
	blockNumberHex := convertToHex(blockNumber)

	var dataToSend = []byte(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["` + blockNumberHex + `", true],"id":1}`)

	fmt.Println("Fetching BlockNumber:", blockNumberHex)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(dataToSend))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Unable to reach the server.")
	} else {
		body, _ = ioutil.ReadAll(resp.Body)
		// fmt.Println("body=", string(body))

	}
	return body
}

func addNode(bdata blockData) {
	fmt.Println("Inside BCD")
	result, _ := db.ExecNeo(
		"CREATE (block:Block {difficulty:{difficulty},ExtraData:{ExtraData},GasLimit:{GasLimit},GasUsed:{GasUsed},Hash:{Hash},LogsBloom:{LogsBloom},Miner:{Miner},MixHash:{MixHash},Nonce:{Nonce},Number:{Number},ParentHash:{ParentHash},ReceiptsRoot:{ReceiptsRoot},Sha3Uncles:{Sha3Uncles},Size:{Size},StateRoot:{StateRoot},Timestamp:{Timestamp},TotalDifficulty :{TotalDifficulty},TransactionsRoot:{TransactionsRoot}})", map[string]interface{}{"difficulty": bdata.Result.Difficulty,
			"ExtraData":        bdata.Result.ExtraData,
			"GasLimit":         bdata.Result.GasLimit,
			"GasUsed":          bdata.Result.GasUsed,
			"Hash":             bdata.Result.Hash,
			"LogsBloom":        bdata.Result.LogsBloom,
			"Miner":            bdata.Result.Miner,
			"MixHash":          bdata.Result.MixHash,
			"Nonce":            bdata.Result.Nonce,
			"Number":           bdata.Result.Number,
			"ParentHash":       bdata.Result.ParentHash,
			"ReceiptsRoot":     bdata.Result.ReceiptsRoot,
			"Sha3Uncles":       bdata.Result.Sha3Uncles,
			"Size":             bdata.Result.Size,
			"StateRoot":        bdata.Result.StateRoot,
			"Timestamp":        bdata.Result.Timestamp,
			"TotalDifficulty":  bdata.Result.TotalDifficulty,
			"TransactionsRoot": bdata.Result.TransactionsRoot})
	numResult, _ := result.RowsAffected()
	fmt.Printf("CREATED ROWS: %d\n", numResult)

	// ---- adding transactions
	transactions := bdata.Result.Transactions

	if len(transactions) != 0 {
		fmt.Println("Transactions Found: ", len(transactions))
		for trans := range transactions {
			transaction := transactions[trans]
			result, _ := db.ExecNeo("CREATE (transaction:Transaction {BlockHash:{BlockHash},BlockNumber:{BlockNumber},From:{From},Gas:{Gas},GasPrice:{GasPrice},Hash:{Hash},Input:{Input},Nonce:{Nonce},To:{To},TransactionIndex:{TransactionIndex},Value:{Value},V:{V},R:{R},S:{S}})", map[string]interface{}{"BlockHash": transaction.BlockHash,
				"BlockNumber":      transaction.BlockNumber,
				"From":             transaction.From,
				"Gas":              transaction.Gas,
				"GasPrice":         transaction.GasPrice,
				"Hash":             transaction.Hash,
				"Input":            transaction.Input,
				"Nonce":            transaction.Nonce,
				"To":               transaction.To,
				"TransactionIndex": transaction.TransactionIndex,
				"Value":            transaction.Value,
				"V":                transaction.V,
				"R":                transaction.R,
				"S":                transaction.S})
			numResult, _ := result.RowsAffected()
			fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 1

			// making relation
			result, _ = db.ExecNeo("Match (block:Block {Hash: {Hash}}) Match (trans: Transaction{ BlockHash: {BlockHash} }) CREATE (block)-[r:HAS_TRANS ]->(trans)", map[string]interface{}{"Hash": bdata.Result.Hash, "BlockHash": transaction.BlockHash})
			numResult, _ = result.RowsAffected()
			fmt.Printf("CREATED ROWS: %d\n", numResult) // CREATED ROWS: 1
		}

	} else {
		fmt.Println("No Transactions found for Block: ", bdata.Result.Hash)
	}

	// create relation  parent -> child
	result, _ = db.ExecNeo("Match (pblock:Block {Hash: {pHash}}) Match (cblock: Block{ Hash: {cHash} }) CREATE (pblock)-[r:HAS_CHILD ]->(cblock)", map[string]interface{}{"pHash": bdata.Result.ParentHash, "cHash": bdata.Result.Hash})
	numResult, _ = result.RowsAffected()
	fmt.Printf("Parent Child Relation: %d\n", numResult) // CREATED ROWS: 1

	// create relation  child -> parent
	result, _ = db.ExecNeo("Match (cblock:Block {Hash: {cHash}}) Match (pblock: Block{ Hash: {pHash} }) CREATE (pblock)-[r:HAS_PARENT ]->(cblock)", map[string]interface{}{"pHash": bdata.Result.Hash, "cHash": bdata.Result.ParentHash})
	numResult, _ = result.RowsAffected()
	fmt.Printf("Child Parent Relation: %d\n", numResult) // CREATED ROWS: 1

}

func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

func main() {

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("Connection Established!")

		blockAddressnums := makeRange(0, 10)

		for address := range blockAddressnums {
			data := getBlockData(blockAddressnums[address])

			var bcd blockData
			err := json.Unmarshal([]byte(data), &bcd)
			if err != nil {
				fmt.Println("ERROR: ", err)
			}

			// fmt.Println("Block:", bcd.Result.Transactions[0])

			addNode(bcd)

		}

	}

	db.Close()

}
