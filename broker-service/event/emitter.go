package event

import (
    "log"
    
    amqp "github.com/rabbitmq/amqp091-go"
)

type Emitter struct {
    connection *amqp.Connection
}

func (e *Emitter) setup() error {
    channel, err := e.connection.Channel()
    if err != nil {
        return err
    }
    
    defer func(channel *amqp.Channel) {
        _ = channel.Close()
    }(channel)
    
    return declareExchange(channel)
}

func (e *Emitter) Push(
    event string,
    severity string,
) error {
    channel, err := e.connection.Channel()
    if err != nil {
        return err
    }
    
    defer func(channel *amqp.Channel) {
        _ = channel.Close()
    }(channel)
    
    log.Println("Pushing to channel")
    
    err = channel.Publish(
        "logs_topic",
        severity,
        false,
        false,
        amqp.Publishing{
            ContentType: "text/plain",
            Body:        []byte(event),
        },
    )
    
    if err != nil {
        return err
    }
    
    return nil
}

func NewEventEmitter(connection *amqp.Connection) (
    Emitter,
    error,
) {
    emitter := Emitter{
        connection: connection,
    }
    
    err := emitter.setup()
    if err != nil {
        return Emitter{}, err
    }
    
    return emitter, nil
}
