SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

docker-image:
	docker build -f ./rabbitmq/rabbitmq.dockerfile -t "tp2_rabbitmq:latest" .
	docker build -f ./inputInterface/consumer.dockerfile -t "tp2_input_interface:latest" .
	docker build -f ./client/producer.dockerfile -t "tp2_client:latest" .
	docker build -f ./filterPto1/filterPto1.dockerfile -t "tp2_filter_pto_1:latest" .
	docker build -f ./percentageCalculator/percentageCalculator.dockerfile -t "tp2_percentage_calculator:latest" .
	docker build -f ./idDelivery/idDelivery.dockerfile -t "tp2_id_delivery:latest" .
	docker build -f ./groupBy/groupBy.dockerfile -t "tp2_group_by:latest" .
	docker build -f ./topTenPto2/topTenPto2.dockerfile -t "tp2_top_ten_pto2:latest" .
	docker build -f ./join/join.dockerfile -t "tp2_join:latest" .
	docker build -f ./duobleGroupBy/duobleGroupBy.dockerfile -t "tp2_duoble_group_by:latest" .
	docker build -f ./topTenPto3/topTenPto3.dockerfile -t "tp2_top_ten_pto3:latest" .
	docker build -f ./endSyncronizer/endSyncronizer.dockerfile -t "tp2_end_syncronizer:latest" .
	
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker-compose -f docker-compose-dev.yaml up producer consumer1 consumer2 consumer3 filterPto1 filterPto1_2 percentageCalculator1  idDelivery groupBy1 groupBy1_2 topTenPto2 join join_2 join_3 duobleGroupBy topTenPto3
.PHONY: docker-compose-up

docker-compose-down:
	docker-compose -f docker-compose-dev.yaml stop -t 1
	docker-compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker-compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs
