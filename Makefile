JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64

run:
	JAVA_HOME=$(JAVA_HOME) PATH="$(JAVA_HOME)/bin:$(PATH)" sbt "run --subscription-file data/local_subscriptions.json --entities-dir data/valid_entities --top-k 10"