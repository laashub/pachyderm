package main

import (
	"archive/tar"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const defaultGroupID = "test"
const defaultTimeout = 5
const defaultNamedPipe = "/pfs/out"

func main() {
	// Get the connection info from the ENV vars
	host := os.Getenv("HOST")
	port := os.Getenv("PORT")
	topic := os.Getenv("TOPIC")

	// Set the default values of the configurable variables
	var (
		group_id = defaultGroupID
		timeout  = defaultTimeout
		pipe     = defaultNamedPipe
	)

	// And create a new kafka reader
	fmt.Printf("creating new kafka reader for %v:%v with topic '%v' and group '%v'\n", host, port, topic, group_id)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{host + ":" + port},
		Topic:    topic,
		GroupID:  group_id,
		MinBytes: 10e1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	// Open the /pfs/out pipe with write only permissons (the pachyderm spout will be reading at the other end of this)
	// Note: it won't work if you try to open this with read, or read/write permissions

	// this is the file loop
	for {
		if err := func() error {
			// read a message
			fmt.Printf("reading kafka queue.\n")
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer func() {
				fmt.Printf("cleaning up context.\n")
				cancel()
			}()
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("opening named pipe %v.\n", pipe)
			// Open the /pfs/out pipe with write only permissons (the pachyderm spout will be reading at the other end of this)
			// Note: it won't work if you try to open this with read, or read/write permissions
			out, err := os.OpenFile(pipe, os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
			defer func() {
				fmt.Printf("closing named pipe %v.\n", pipe)
				out.Close()
			}()

			fmt.Printf("opening tarstream\n")
			tw := tar.NewWriter(out)
			defer func() {
				fmt.Printf("closing tarstream.\n")
				tw.Close()
			}()

			fmt.Printf("processing header for topic %v @ offset %v\n", m.Topic, m.Offset)
			// give it a unique name
			name := fmt.Sprintf("%v-%v", m.Topic, m.Offset)
			// write the header
			for err = tw.WriteHeader(&tar.Header{
				Name: name,
				Mode: 0600,
				Size: int64(len(m.Value)),
			}); err != nil; {
				if !strings.Contains(err.Error(), "broken pipe") {
					return err
				}
				// if there's a broken pipe, just give it some time to get ready for the next message
				fmt.Printf("broken pipe\n")
				time.Sleep(time.Duration(timeout) * time.Millisecond)
			}
			fmt.Printf("processing data for topic  %v @ offset %v\n", m.Topic, m.Offset)
			// and the message
			for _, err = tw.Write(m.Value); err != nil; {
				if !strings.Contains(err.Error(), "broken pipe") {
					return err
				}
				// if there's a broken pipe, just give it some time to get ready for the next message
				fmt.Printf("broken pipe\n")
				time.Sleep(time.Duration(timeout) * time.Millisecond)
			}
			return nil
		}(); err != nil {
			panic(err)
		}
	}
}
