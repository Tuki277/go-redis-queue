package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/adjust/rmq/v5"
	"github.com/urfave/cli/v2"
)

func main() {
	connection, err := rmq.OpenConnection("my service", "tcp", "localhost:6379", 1, nil)
	if err != nil {
		log.Fatal(err)
	}

	taskQueue, err := connection.OpenQueue("tasks")
	if err != nil {
		log.Fatal(err)
	}

	app := &cli.App{
		Name:  "rmq",
		Usage: "run a redis queue",
		Commands: []*cli.Command{
			{
				Name:  "producer",
				Usage: "create redis queue producer",
				Action: func(ctx *cli.Context) error {
					fmt.Println("I AM THE PRODUCER")

					go func() {
						ticker := time.NewTicker(1 * time.Second)
						i := 0
						for {
							select {
							case <-ticker.C:
								payload := fmt.Sprintf("task %d", i)
								fmt.Println(payload)
								if err := taskQueue.Publish(payload); err != nil {
									log.Fatal(err)
								}
							}
							i++
						}
					}()

					http.Handle("/overview", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						q, err := connection.GetOpenQueues()
						if err != nil {
							log.Fatal(err)
						}
						stats, err := connection.CollectStats(q)
						if err != nil {
							log.Fatal(err)
						}
						fmt.Fprintf(w, stats.GetHtml("", ""))
					}))

					if err := http.ListenAndServe(":333", nil); err != nil {
						log.Fatal(err)
					}

					return nil
				},
			},
			{
				Name:  "consumer",
				Usage: "create redis queue consumer",
				Action: func(ctx *cli.Context) error {
					fmt.Println("I AM THE CONSUMER")
					taskQueue.StartConsuming(10, time.Second)
					taskQueue.AddConsumerFunc("task-consumer", func(d rmq.Delivery) {
						log.Printf("[%s]: performing task %s", ctx.Args().First(), d.Payload())

						if err := d.Ack(); err != nil {
							log.Fatal(err)
						}
					})

					return nil
				},
			},
		},
	}

	app.Run(os.Args)

	time.Sleep(1 * time.Hour)
}
