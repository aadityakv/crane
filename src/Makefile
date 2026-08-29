GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

MAIN_BINARY=ece428
GREP_BINARY=ece428_grep

all: build build_grep
build:
	$(GOBUILD) -o $(MAIN_BINARY) -v
build_grep:
	$(GOBUILD) -o $(GREP_BINARY) -v grep_client
test:
	$(GOTEST) -v ./...
setup:
	$(GOGET) github.com/fatih/color
	cd ../tools; ./send_bashrc.sh $(USER)
clean:
	$(GOCLEAN)
	rm -f $(MAIN_BINARY)
	rm -f $(GREP_BINARY)
run:
	$(GOBUILD) -o $(BINARY_NAME) -v ./...
	./$(BINARY_NAME)
