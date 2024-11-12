package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

const bufferSize = 1024

var errorCode = map[string] int16 {
	"unsupportedVersion": 35,
}

type reqHeader struct {
	reqAPIKey int16
	reqAPIVer int16 
	correlationID int32
	clientID string 

}

type msgReceive struct {
	msgSize int32
	header reqHeader
}

type msgResponse struct {
	msgSize int32
	correlationID int32
	errorCode int16
	// body string
}

func (m *msgResponse) format() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, m.msgSize); err != nil {
			return nil, err
	}

	// Write CorrelationID in 4 bytes
	if err := binary.Write(buf, binary.BigEndian, m.correlationID); err != nil {
			return nil, err
	}

	// Write Error Code in 2 Bytes
	if err := binary.Write(buf, binary.BigEndian, m.errorCode); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func parseBuffer(data []byte) (*msgReceive, error) {
	var msgReceive msgReceive

	buf := bytes.NewReader(data)

	if err := binary.Read(buf, binary.BigEndian, &msgReceive.msgSize); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &msgReceive.header.reqAPIKey); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &msgReceive.header.reqAPIVer); err != nil {
		return nil, err
	}

	if err := binary.Read(buf, binary.BigEndian, &msgReceive.header.correlationID); err != nil {
		return nil, err
	}

	return &msgReceive , nil
}

func createResponse(src msgReceive) *msgResponse {
	var dest msgResponse
	dest.correlationID = src.header.correlationID
	dest.msgSize = src.msgSize

	if src.header.reqAPIVer < 0 || src.header.reqAPIVer > 4 {
		dest.errorCode = errorCode["unsupportedVersion"]
	}

	return &dest
}

func handleClientConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Start of handle client")

	buffer := make([]byte, bufferSize)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Failed to read from client. Error message: ", err.Error())
	}

	fmt.Println("Received: ", buffer[:n])

	m, err := parseBuffer(buffer)
	if err != nil {
		fmt.Println("Failed to parse client. Error message: ", err.Error())
	}

	response := createResponse(*m)
	responseBytes, err := response.format()
	if err != nil {
		fmt.Println("Failed to format message. Error message: ", err.Error())
	}

	// Print each byte in hexadecimal format
	fmt.Println("sending: ")
	for i, b := range responseBytes {
		fmt.Printf("%02X ", b)
		if (i+1)%4 == 0 {
				fmt.Println() // New line after every 4 bytes for readability
		}
}

	conn.Write(responseBytes)
}


func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Starting to accept")

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	fmt.Println("After accept")


	handleClientConnection(conn)
}
