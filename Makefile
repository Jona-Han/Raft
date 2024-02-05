# Use the following makefile to build your project, for example:
# 1. To build the whole lab1 (builds mrapps, mrcoordinator, mrworker, mrsequential)
#    $ make lab1
# 2. To build the mrapps package (builds all the mrapps)
#    $ make build-mrapps
# 3. To build a specific mrapp, for exmaple wc:
#	 $ make build-mrapp-wc
# 4. To build the mr package
#    $ make build-mr
# 5. To make a tar for lab1 for student id "87654321"
#	 $ make tar lab_name=lab1 student_id=i_87654321

MRAPPS_SRC_DIR = src/mrapps
MAIN_SRC_DIR = src/main

# List of mrapps to build
MRAPPS = wc crash early_exit indexer jobcount mtiming nocrash rtiming

.PHONY: tar lab1

build-mrapps: $(addprefix build-mrapp-, $(MRAPPS))

build-mrapp-%:
	@echo "-- building $*"
	cd $(MRAPPS_SRC_DIR) && go build -buildmode=plugin $*.go

build-mr:
	@echo "-- building mrcoordinator mrworker mrsequential"
	cd $(MAIN_SRC_DIR) && go build mrcoordinator.go
	cd $(MAIN_SRC_DIR) && go build mrsequential.go
	cd $(MAIN_SRC_DIR) && go build mrworker.go

lab1: build-mrapps build-mr
	@echo "-- lab1 built successfully"

lab2: 
	@echo "-- skipping the build phase for lab2a"

tar: $(lab_name)
	@echo "preparing a tar for $(lab_name) for student $(student_id)"
	
	tar \
	--exclude=src/main/*.txt \
	--exclude='.git' \
	--exclude='*.so' \
	--exclude='*.tar.gz' \
	--exclude='mrcoordinator' \
	--exclude='mrworker' \
	--exclude='mrsequential' \
	-cvzf \
	$(student_id).tar.gz ./src ./Makefile
 	
	@if tar tzf $(student_id).tar.gz > /dev/null 2>&1; then \
		echo "Tar file created and is valid."; \
	else \
        echo "Error creating or validating tar file."; \
        rm -f $(student_id).tar.gz; \
        exit 1; \
    fi

clean:
	rm -f $(MAIN_SRC_DIR)/mrcoordinator $(MAIN_SRC_DIR)/mrsequential $(MAIN_SRC_DIR)/mrworker
	rm -f $(MRAPPS_SRC_DIR)/*.so
