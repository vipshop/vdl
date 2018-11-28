all: build

build: vdl snap
vdl:
	go build -o ./bin/vdl ./cmd/vdl
	go build -o ./bin/vdlctl ./cmd/vdlctl
	#@bash genver.sh
snap:
	mkdir -p ./bin/snap/server
	mkdir -p ./bin/snap/agent
	go build -o ./bin/snap/server/send_server ./tools/snapshot/server
	go build -o ./bin/snap/agent/receive_agent ./tools/snapshot/agent
	go build -o ./bin/snap/agent/restore_agent ./tools/snapshot/restore
	cp ./tools/snapshot/agent.sh ./bin/snap/agent
	cp ./tools/snapshot/config.sh ./bin/snap/agent
clean:
	@rm -rf bin

test:
	go test ./go/... -race
