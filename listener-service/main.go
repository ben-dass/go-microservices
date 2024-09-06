package main

import (
    "fmt"
    "log"
    "math"
    "os"
    "time"
    
    amqp "github.com/rabbitmq/amqp091-go"
    "listener/event"
)

func main() {
    
    // connect
    rabbitConn, err := connect()
    if err != nil {
        log.Println(err)
        os.Exit(1)
    }
    
    defer func(rabbitConn *amqp.Connection) {
        _ = rabbitConn.Close()
    }(rabbitConn)
    
    // start listening
    log.Println("Listening for and consuming RabbitMQ messages...")
    
    // create consumer
    consumer, err := event.NewConsumer(rabbitConn)
    if err != nil {
        panic(err)
    }
    
    // watch the queue and consume events
    err = consumer.Listen([]string{"log.INFO", "log.WARNING", "log.ERROR"})
    if err != nil {
        log.Println(err)
    }
}

func connect() (*amqp.Connection, error) {
    var counts int64
    var backoff = 1 * time.Second
    var connection *amqp.Connection
    
    for {
        c, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
        if err != nil {
            fmt.Println("RabbitMQ not yet ready...")
            counts++
        } else {
            log.Println("Connected to RabbitMQ")
            connection = c
            break
        }
        
        if counts > 5 {
            fmt.Println(err)
            return nil, err
        }
        
        backoff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
        log.Println("backing off...")
        time.Sleep(backoff)
    }
    
    return connection, nil
}
