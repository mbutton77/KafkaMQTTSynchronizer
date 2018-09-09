package fr.mbutton.kafka.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQTT2Kafka {

	private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

	public static String MQTT_BROKER_URL      = "tcp://192.168.0.232:1883";
	public static String MQTT_BROKER_USER     = "jeedom";
	public static String MQTT_BROKER_PASSWORD = "jeedom";

	public static String KAFKA_BOOTSTRAP      = "localhost:9092";
	public static String KAFKA_TOPIC          = "sensor.counter";

	public void synchronize() {
		final Properties props = new Properties();
		props.put("bootstrap.servers", KAFKA_BOOTSTRAP);
		props.put("key.serializer"   , "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

		MemoryPersistence persistence = new MemoryPersistence();

		MqttClient mqttClient = null; // not auto-closeable
		try {
			mqttClient = new MqttClient(MQTT_BROKER_URL, "Synchronizer", persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);

			mqttClient.setCallback(new MqttCallback() {
				@Override
				public void connectionLost(Throwable cause) {
					logger.warn("Connection lost ¯\\_(ツ)_/¯ : please restart me !");
				}

				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception {
					logger.debug("Message: " + message.toString());
					sendMessageOverKafka(topic, message);
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
					logger.debug("Job's done.");
				}

				private void sendMessageOverKafka(String topic, MqttMessage message) {
					logger.debug("Sending the message to my Kafka cluster.");
					List<Header> headers = new ArrayList<>();
					headers.add(new RecordHeader("mqtt-topic", topic.getBytes()));
					try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
						ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, 0, "1", message.toString(), headers);
						producer.send(record);
					}
				}
			});

			mqttClient.connect(connOpts);
			mqttClient.subscribe("#");

		} catch (MqttException e) {
			logger.error("Little problem while trying to synchronize MQTT gateway with Kafka broker.", e);
		} finally {
			if (mqttClient != null) {
				try {
					mqttClient.close();
				} catch (MqttException ex) {
					logger.error("Error while liberating the resources.", ex);
				}
			}
		}
	}

	public static void main(String[] args) {
		MQTT2Kafka bridge = new MQTT2Kafka();
		bridge.synchronize();
	}
}
