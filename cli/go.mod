module github.com/das08/matlab-jobqueue/cli

go 1.19

replace github.com/das08/matlab-jobqueue/connector => ../connector

require github.com/das08/matlab-jobqueue/connector v0.0.0-00010101000000-000000000000

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/redis/go-redis/v9 v9.3.0 // indirect
)
