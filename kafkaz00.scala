import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.{Arrays, Properties}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.concurrent.Future
import java.nio.charset.StandardCharsets

object KafkaTopicCreator {
  def main(args: Array[String]): Unit = {
    // Define Kafka properties
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    // Create AdminClient
    val adminClient = AdminClient.create(config)

    // Define topics
    val topics = Arrays.asList(
      new NewTopic("streams-plaintext-input", 1, 1.toShort),
      new NewTopic("streams-wordcount-output", 1, 1.toShort)
    )

    // Create topics asynchronously
    val createTopicsFuture = Future {
      adminClient.createTopics(topics).all()
    }

    createTopicsFuture.onComplete {
      case Success(_) =>
        println("Topics created successfully.")
        generateInputFile()  // Call function to generate input file
        sendFileToKafka("streams-plaintext-input", "/tmp/file-input.txt")  // Publish messages
      case Failure(ex) =>
        println(s"Error creating topics: ${ex.getMessage}")
    }

    // Close AdminClient
    adminClient.close()
  }

  // Function to generate Kafka input file
  def generateInputFile(): Unit = {
    val filePath = Paths.get("/tmp/file-input.txt")
    val content = "all streams lead to kafka\nhello kafka streams\njoin kafka summit\n"

    try {
      Files.write(filePath, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      println(s"Input file generated at: $filePath")
    } catch {
      case e: Exception => println(s"Error writing file: ${e.getMessage}")
    }
  }

  // Function to send file contents to Kafka
  def sendFileToKafka(topic: String, filePath: String): Unit = {
    // Kafka producer properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](producerProps)

    try {
      val lines = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8)
      lines.forEach { line =>
        producer.send(new ProducerRecord[String, String](topic, line))
        println(s"Sent to Kafka: $line")
      }
    } catch {
      case e: Exception => println(s"Error sending file to Kafka: ${e.getMessage}")
    } finally {
      producer.close()
    }
  }
}
