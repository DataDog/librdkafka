KAFKA_HOME ?=
BUILD_DIR := build
CLASS_NAME := RecordExtract

ifeq ($(strip $(KAFKA_HOME)),)
$(error Set KAFKA_HOME to the root of a Kafka distribution, e.g. the kafka_2.13-3.4.1 directory)
endif

CLASSPATH := $(BUILD_DIR):$(KAFKA_HOME)/libs/*

.PHONY: all run clean

all: $(BUILD_DIR)/$(CLASS_NAME).class

$(BUILD_DIR)/$(CLASS_NAME).class: $(CLASS_NAME).java
	mkdir -p $(BUILD_DIR)
	javac -cp "$(KAFKA_HOME)/libs/*" -d $(BUILD_DIR) $(CLASS_NAME).java

run: all
	java -cp "$(CLASSPATH)" $(CLASS_NAME) $(ARGS)

clean:
	rm -rf $(BUILD_DIR)
