package org.flame.jms.client.test;

import static org.junit.Assert.*;

import javax.jms.ObjectMessage;

import org.flame.jms.client.helper.Credential;
import org.flame.jms.client.helper.IJmsMessageExecutor;
import org.flame.jms.client.helper.ITransaction;
import org.flame.jms.client.helper.JMSImplementation;
import org.flame.jms.client.remote.AbstractJmsMessageExecutor;
import org.flame.jms.client.remote.CorrelationIDGenerator;
import org.flame.jms.client.remote.IJmsConnectionConfiguration;
import org.flame.jms.client.remote.IJmsQueueConfiguration;
import org.flame.jms.client.remote.IMessageConnection;
import org.flame.jms.client.remote.IMessageSession;
import org.flame.jms.client.remote.JMSMessageProducer;
import org.flame.jms.client.remote.JmsTransportConfiguration;
import org.flame.jms.exceptions.FlameJMSClientException;
import org.flame.jms.exceptions.FlameMessageExecutionException;
import org.flame.jms.exceptions.FlameMessageFlowException;
//import org.apache.activemq.ActiveMQConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJmsClient {
	private static Credential credential = null;
	private static JmsTransportConfiguration transportConf = null;
	private static IJmsConnectionConfiguration connectionConf = null;
	private static IJmsQueueConfiguration queueConfig = null;
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String connUserName = "prabir";
		String connPassword = "prabir4@mj";
		
		credential = new Credential(connUserName, connPassword);
		transportConf = new JmsTransportConfiguration(JMSImplementation.HORNETQ,  "127.0.0.1", credential);
		connectionConf = transportConf.createConnectionConfiguration("jms/RemoteConnectionFactory", credential);
		queueConfig = transportConf.createQueueConfiguration("jms/queue/asyncInbound");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		transportConf.release();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	//@Test
	public void testHornetQNonTransactional() {
		try {
			
			IJmsMessageExecutor<Void> executor = new AbstractJmsMessageExecutor<Void>(
					connectionConf, queueConfig) {

				public Void execute() throws FlameMessageFlowException {
					ObjectMessage objectMsg = createObjectMessage(new AppMessage());
					getProducer().send(objectMsg);
					return null;
				}
			};
			executor.run();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testHornetQTransactional() {
		try {
			
			boolean isTransactional = true;
			IJmsMessageExecutor<Void> executor = new AbstractJmsMessageExecutor<Void>(
					connectionConf, queueConfig, isTransactional) {

				public Void execute() throws FlameMessageFlowException {
					ObjectMessage objectMsg = createObjectMessage(new AppMessage());
					
					getProducer().send(objectMsg);
					return null;
				}
			};
			executor.run();
		} catch (FlameMessageExecutionException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	//@Test
	public void testHornetQTransactionalWithBeginTrxn() {
		try {
			
			boolean isTransactional = true;
			IJmsMessageExecutor<Void> executor = new AbstractJmsMessageExecutor<Void>(
					connectionConf, queueConfig, isTransactional) {

				public Void execute() throws FlameMessageFlowException {
					ObjectMessage objectMsg = createObjectMessage(new AppMessage());
					ITransaction txn = beginTransaction();
					getProducer().send(objectMsg);
					txn.commit();
					return null;
				}
			};
			executor.run();
		} catch (FlameMessageExecutionException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	//@Test
	public void testHornetQTransactionalUnCommitted() {
		try {
			
			boolean isTransactional = true;
			IJmsMessageExecutor<Void> executor = new AbstractJmsMessageExecutor<Void>(
					connectionConf, queueConfig, isTransactional) {

				public Void execute() throws FlameMessageFlowException {
					ObjectMessage objectMsg = createObjectMessage(new AppMessage());
					ITransaction txn = beginTransaction();
					getProducer().send(objectMsg);
					//txn.commit();
					return null;
				}
			};
			executor.run();
		} catch (FlameMessageExecutionException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testHornetQUnStructured() {
		ITransaction txn = null;
		try {
			IMessageConnection conn = connectionConf.getConnection();
			IMessageSession session = conn.createTransactionalSession();
			JMSMessageProducer producer = session.createProducer(queueConfig);
			txn = session.beginTransaction();
			
			ObjectMessage objectMsg = session.createObjectMessage(new AppMessage());
			
			producer.send(objectMsg);
			txn.commit();

			producer.close();
			session.close();
			
		} catch (Exception e) {
			if(txn!=null)
			{
				txn.rollback();
			}
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testHornetQWithUserDefineCorrelationIDGenerator() {
		ITransaction txn = null;
		try {
			IMessageConnection conn = connectionConf.getConnection();
			IMessageSession session = conn.createTransactionalSession();
			JMSMessageProducer producer = session.createProducer(queueConfig);
			txn = session.beginTransaction();
			
			ObjectMessage objectMsg = session.createObjectMessage(new AppMessage(), new CorrelationIDGenerator() {
				
				@Override
				public String generate() {
					
					return "Correlation : " + 1;
				}
			});
			
			producer.send(objectMsg);
			txn.commit();

			producer.close();
			session.close();
			
		} catch (Exception e) {
			if(txn!=null)
			{
				txn.rollback();
			}
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testActiveMQ() {
		try {
			String connUserName = "admin";
			String connPassword = "admin";
			
			Credential credential = new Credential(connUserName, connPassword);
			JmsTransportConfiguration transportConf2 = new JmsTransportConfiguration(JMSImplementation.ACTIVEMQ,  "localhost", credential);
			IJmsConnectionConfiguration connectionConf2 = transportConf2.createConnectionConfiguration("ConnectionFactory", credential);
			IJmsQueueConfiguration queueConfig2 = transportConf2.createQueueConfiguration("APPANALYZER_QUEUE");
	
			IJmsMessageExecutor<Void> executor2 = new AbstractJmsMessageExecutor<Void>(
					connectionConf2, queueConfig2) {
	
				public Void execute() throws FlameMessageFlowException {
					AppMessage msg = new AppMessage();
					msg.setName("Prabir");
					ObjectMessage objectMsg = createObjectMessage(msg);
					getProducer().send(objectMsg);
					return null;
				}
			};
			executor2.run();
		} catch (FlameMessageExecutionException e) {
			e.printStackTrace();
			Assert.fail();
		} catch(FlameJMSClientException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
}
