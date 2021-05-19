to build run: `cd service ` then `go build`

to start the service you can run: `./service` the service defaults to port 1337

you can override the port it listens to by passing the port flag like this `./service --port=1338`


i created a local docker file for postgres and ran it on a custom port running the following command:
`docker run -d --name mailgun-postgres -e POSTGRES_PASSWORD=Pass2020! -v ${HOME}/postgres-data/:/var/lib/postgresql/data -p 6001:5432 postgres`

then rerunning that docker with this command:
`docker start <uuid of mailgun-postgres>`


Design 
The code is a simple implementation that can be scaled horizontally

The major issue with this system is the massive amount of writes being performed. 
Generally systems are dominated by reads, facebook/amazon etc. 
However this system is dominated by writes

Since the writes are updating/creating a domain with 1 value we can reduce the writes by adding a simple
messaging queue that could aggregate writes over a certain time interval and then flush those updates. 
![Alt text](pictures/code_diagram.png?raw=true "Code Diagram")

We can expand this design further by adding a loadbalancer between the client, and the service

![Alt text](pictures/scale1.png?raw=true "Scale Option 1")

We can extend this design also by adding read shards for the database to reduce the load on the
master write DB

![Alt text](pictures/scale2.png?raw=true "Scale Option 2")

We can still improve upon this design by taking advantage of the domain rules. We can split the datapath
into starting letters One database could hold all the domains starting with an A then another with B ...until we reach Z


![Alt text](pictures/scale3.png?raw=true "Scale Option 3")

Lastly we could further take advantage of the domain rules by observing where the domain is geographically located.
We can put any previous design inside a datacenter where a load balancer controls which DC it could divert to.
THe customer could then direct local loads to a locally established data center. so US domains would go to the U.S. DC's
UK would go to UK DCs and so on. 
![Alt text](pictures/scale4.png?raw=true "Scale Option 4")

Other things we could consider to help with the load.
1.) We could abstract the message queue storage to a cache service like Redis and have the queue pull from there
2.) We could use a connection pooler like PGBouncer to save on the latency of connecting to the datbase. THis is one reason i chose pgx as in
adapts to both raw and PG Bouncer connections


Sanity test results:
Delivered
![Alt text](pictures/test1.png?raw=true "Delivered")
Bounced
![Alt text](pictures/test2.png?raw=true "Bounced")

Get
![Alt text](pictures/test3.png?raw=true "GET")

Database updates:
![Alt text](pictures/db_updates.png?raw=true "GET")

