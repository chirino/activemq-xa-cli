package org.fusesource.xacli;

import io.airlift.command.*;
import org.apache.activemq.ActiveMQXAConnectionFactory;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

/**
 */
public class Main {

    public static void main(String[] args) {
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder("activemq-xa-cli")
                .withDescription("the XA CLI tool")
                .withDefaultCommand(Help.class)
                .withCommands(
                    Help.class,
                    ListTransactions.class,
                    Create.class,
                    CommitTransaction.class,
                    RollbackTransaction.class
                );

        Cli<Runnable> gitParser = builder.build();
        gitParser.parse(args).run();
        System.exit(0);
    }

    abstract public static class BaseCommand implements Runnable {
        @Option(type = OptionType.GLOBAL, name = "-v", description = "Verbose mode")
        public boolean verbose;

        @Option(type = OptionType.GLOBAL, name = "-b", description = "Broker URL, defaults to tcp://localhost:61616")
        public String url = "tcp://localhost:61616";

        @Option(type = OptionType.GLOBAL, name = "-u", description = "Username")
        public String user;

        @Option(type = OptionType.GLOBAL, name = "-p", description = "Password")
        public String password;


        protected void verbose(String msg) {
            if( verbose ) {
                System.out.println(msg);
            }
        }

        protected void println(Throwable error) {
            if( verbose ) {
                error.printStackTrace();
            } else {
                System.err.println("FAILED: "+error);
                System.exit(2);
            }
        }
        protected void println(String msg) {
            System.out.println(msg);
        }

        protected XAConnection createConnection() throws JMSException {
            ActiveMQXAConnectionFactory factory = new ActiveMQXAConnectionFactory(url);
            factory.setUserName(user);
            factory.setPassword(password);
            verbose("Connecting to: " + url);
            XAConnection xaConnection = factory.createXAConnection();
            xaConnection.start();
            verbose("Connected");
            return xaConnection;
        }

        static String toString(Xid value) {
            StringBuilder rc = new StringBuilder();
            rc.append(String.format("%016X", value.getFormatId()));
            rc.append(":");
            rc.append(toString(value.getGlobalTransactionId()));
            rc.append(":");
            rc.append(toString(value.getBranchQualifier()));
            return rc.toString();
        }

        static String toString(byte[] value) {
            StringBuilder rc = new StringBuilder();
            for (byte b : value) {
                rc.append(String.format("%02X", b));
            }
            return rc.toString();
        }

        public void run() {
            XAConnection connection = null;
            try {
                connection = createConnection();
                XASession xaSession = connection.createXASession();
                XAResource xaResource = xaSession.getXAResource();
                run(connection, xaSession, xaResource);
            } catch (Exception e) {
                println(e);
            } finally {
                verbose("Closing the connection");
                try {
                    connection.close();
                } catch (JMSException e) {
                }
            }
        }

        abstract protected void run(XAConnection connection, XASession xaSession, XAResource xaResource) throws Exception;
    }

    @Command(name = "create", description = "Creates a prepared XA transaction")
    public static class Create extends BaseCommand {
        @Option(name = "-q", description = "Queue name, defaults to TEST")
        public String queue = "TEST";

        @Override
        protected void run(XAConnection connection, XASession xaSession, XAResource xaResource) throws Exception {
            verbose("Starting XA transaction");
            Xid xid = createXid();
            xaResource.start(xid, 0);

            verbose("Sending message");
            MessageProducer producer = xaSession.createProducer(xaSession.createQueue(queue));
            producer.send(xaSession.createTextMessage("TEST"));

            verbose("Ending XA transaction");
            xaResource.end(xid, XAResource.TMSUCCESS);

            verbose("Preparing XA transaction");
            xaResource.prepare(xid);

            println("Created: "+toString(xid));
        }
    }

    @Command(name = "list", description = "List transactions")
    public static class ListTransactions extends BaseCommand {
        @Override
        protected void run(XAConnection connection, XASession xaSession, XAResource xaResource) throws Exception {
            verbose("Getting prepared transactions");
            Xid[] recover = xaResource.recover(0);

            println("Found " + recover.length + " prepared transactions");
            for (Xid xid : recover) {
                println(toString(xid));
            }
        }
    }

    @Command(name = "commit", description = "Commit a transaction")
    public static class CommitTransaction extends BaseCommand {

        @Arguments(description = "Prepared transaction ids to commit")
        public List<String> ids;


        @Override
        protected void run(XAConnection connection, XASession xaSession, XAResource xaResource) throws Exception {
            verbose("Getting prepared transactions");
            Xid[] recover = xaResource.recover(0);

            HashSet<String> remaining = new HashSet<String>(ids);
            for (Xid xid : recover) {
                String id = toString(xid);
                if( remaining.remove(id) ) {
                    println("Committing: "+id);
                    xaResource.commit(xid, false);
                }
            }

            if( !remaining.isEmpty() ) {
                for (String id : remaining) {
                    println("Not found: "+id);
                }
            }
        }

    }

    @Command(name = "rollback", description = "Rollback a transaction")
    public static class RollbackTransaction extends BaseCommand {

        @Arguments(description = "Prepared transaction ids to rollback")
        public List<String> ids;

        @Override
        protected void run(XAConnection connection, XASession xaSession, XAResource xaResource) throws Exception {
            verbose("Getting prepared transactions");
            Xid[] recover = xaResource.recover(0);

            HashSet<String> remaining = new HashSet<String>(ids);
            for (Xid xid : recover) {
                String id = toString(xid);
                if( remaining.remove(id) ) {
                    println("Rolling back: "+id);
                    xaResource.rollback(xid);
                }
            }

            if( !remaining.isEmpty() ) {
                for (String id : remaining) {
                    println("Not found: "+id);
                }
            }
        }
    }

    static long txGenerator = System.currentTimeMillis();
    static public Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            public int getFormatId() {
                return 86;
            }

            public byte[] getGlobalTransactionId() {
                return bs;
            }

            public byte[] getBranchQualifier() {
                return bs;
            }
        };

    }
}
