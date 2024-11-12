package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

const bufferSize = 1024


type msg struct {
	msgSize int32
	correlationID int32
	// body string
}

func (m *msg) format() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, m.msgSize); err != nil {
			return nil, err
	}

	// Write CorrelationID in 4 bytes
	if err := binary.Write(buf, binary.BigEndian, m.correlationID); err != nil {
			return nil, err
	}

	return buf.Bytes(), nil
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

	m := msg{
		msgSize: 0,
		correlationID: 7,
	}
	
	mBytes, err := m.format()
	if err != nil {
		fmt.Println("Failed to format message. Error message: ", err.Error())
	}

	// Print each byte in hexadecimal format
	fmt.Println("sending: ")
	for i, b := range mBytes {
		fmt.Printf("%02X ", b)
		if (i+1)%4 == 0 {
				fmt.Println() // New line after every 4 bytes for readability
		}
}

	conn.Write(mBytes)
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
