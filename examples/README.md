# EXAMPLES


## chatViaProxy


**ChatServer.java** is a simple web server with a REST api.

You can connect directly to port 8000 and run the web application

It also includes a Nabu instance and prints the listening address on startup.

**Sender.java** is another web server running on port 9000

The main method has a variable that should be set to the listening address of ChatServer

It also includes a Nabu instance used to proxy requests to the Nabu instance in ChatServer

**Diagram**

www -> (http server & node1)[*Sender*] \~\~\~\~\~ (node2 & http server)[*ChatServer*]

As a user you can interact with the chat web application directly (port 8000).

Or via whatever host that has an instance of Sender running (port 9000).

In this case a request is proxied to the chat web application via a p2p stream between Nabu instances.