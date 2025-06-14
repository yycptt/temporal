# Example Payload Store

This example payload store is a simple implementation that allows you to store and retrieve payloads using a dictionary. It is designed to be used with the Chasm framework for testing purposes.

It currently only support adding string payloads.

### Usage
To use this example payload store, you use the following tdbg commands;
```bash
// start the server
make start 

// register a default namespace
./tctl n re 

// Create a new payload store with ID "my-test-store"
./tdbg payload new --store-id my-test-store

// Add a new string payload with key "key1" and value "value1" to the store
./tdbg payload add --store-id my-test-store --key key1 --payload value1

// Retrieve the payload with key "key1" from the store
./tdbg payload get --store-id my-test-store --key key1

// Describe the payload store
./tdbg payload describe --store-id my-test-store

// Remove the payload with key "key1" from the store
./tdbg payload remove --store-id my-test-store --key key1
```
