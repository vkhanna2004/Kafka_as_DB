package server

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Command struct {
	Name string
	Args []string
}

func ParseCommand(reader *bufio.Reader) (*Command, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != '*' {
		return nil, fmt.Errorf("expected '*', got %q", b)
	}
	//consumed '*' till here, now consume length of array

	// Read array length (e.g. "3\r\n")
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	count, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	parts := make([]string, 0, count)
	for i := 0; i < count; i++ {
		//consumed $
		s, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if s != '$' {
			return nil, fmt.Errorf("expected '$', got %q", s)
		}

		//consume length of bulk string e.g. $5\r\nhello\r\n
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}
		// Read the actual string bytes e.g. hello
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}

		// Consume trailing "\r\n"
		crlf := make([]byte, 2)
		if _, err := io.ReadFull(reader, crlf); err != nil {
			return nil, err
		}

		parts = append(parts, string(data))
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: parts[0],
		Args: parts[1:],
	}, nil
}
