# activemq-xa-cli

## Description

A little command line interface tool to create, list, commit, or rollback
XA transactions in an [Apache ActiveMQ](http://activemq.apache.org) message 
broker.

## Building

To build, you must have [maven](http://maven.apache.org/download.cgi) installed then
run:

    mvn package
    
The jar will be built to: `target/activemq-xa-cli-1.0-SNAPSHOT.jar`

## Usage

To run:

    java -jar target/activemq-xa-cli-1.0-SNAPSHOT.jar

The tool's help screen:

    $ java -jar target/activemq-xa-cli-1.0-SNAPSHOT.jar help
    usage: activemq-xa-cli [-b <url>] [-p <password>] [-u <user>] [-v] <command>
            [<args>]

    The most commonly used activemq-xa-cli commands are:
        commit     Commit a transaction
        create     Creates a prepared XA transaction
        help       Display help information
        list       List transactions
        rollback   Rollback a transaction

    See 'activemq-xa-cli help <command>' for more information on a specific command.
