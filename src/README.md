# Setup

These instructions have been modified from the original project to reflect the adoption of modules since Go 1.16
- Use `go mod init` to create module
- No more tinkering with `GOPATH`

To run (each command in separate shell):
```
$ go run src/master/master.go
$ go run src/server/server.go -l -port 7070
$ go run src/server/server.go -l -port 7071
$ go run src/server/server.go -l -port 7072
$ go run src/client/client.go
```

