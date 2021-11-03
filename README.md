Ejecutar el comando **make docker-compose-up** para comenzar la ejecución del ejemplo y luego
el comando **make docker-compose-logs** para poder observar los logs de las aplicaciones

Hasta ahora estan funcionando el punto 1 y el punto 3 por separado.
Para correr el programa primero ejecutar el comando **make docker-image** para buildear las imagenes de docker. Luego levantar rabbit con el comando **docker-compose -f docker-compose-dev.yaml up rabbitmq**. 
Una vez iniciado rabbit se peude correr el ejercicio 1 con el comando **docker-compose -f docker-compose-dev.yaml up producer consumer1 filterPto1 percentageCalculator1** o el ejercicio 2 usando el comando **docker-compose -f docker-compose-dev.yaml up producer consumer1 join duobleGroupBy topTenPto3**