**We can read from a Kafka topic from a specific timestamp using Spark Structured Streaming or Batch mode**.

Since you want a **Scala Spark batch application for data reconciliation**, here's exactly how to **read Kafka data from a specific timestamp** using **Spark 3.3.0** (or similar).

---

## ✅ **Key Concepts**

* For **batch reads**, you use `.read.format("kafka")`.
* You control the starting point using `.option("startingOffsets", s"""{"topic":{"0":{"offset":-1}}}""")` or by **timestamp lookup**.
* Spark allows you to **query Kafka offsets by timestamp**, but you **first have to determine offsets** corresponding to your **desired timestamp**.

---

### ✅ **Approach Breakdown**

| Step | Action                                                                          |
| ---- | ------------------------------------------------------------------------------- |
| 1️⃣  | Query Kafka to get offsets for the **timestamp**                                |
| 2️⃣  | Use `.option("startingOffsets")` and `.option("endingOffsets")` to define range |
| 3️⃣  | Perform **batch read** and your reconciliation logic                            |

---

## ✅ **Step 1: Get Kafka Offsets for Timestamp (Kafka CLI)**

Example for timestamp = **July 9, 2025 00:00:00**:

```bash
kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic test_topic \
  --time 1720468800000
```

(`1720468800000` is timestamp in milliseconds)

This will return offsets from **that timestamp** for each partition.

---

## ✅ **Step 2: Scala Code – Batch Read from Specific Offset Range**

```scala
import org.apache.spark.sql.SparkSession

object KafkaBatchReconciliation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("KafkaBatchRecon")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Example starting offsets (from timestamp lookup), endingOffsets can be latest or another timestamp
    val startingOffsets =
      """
        |{
        |  "test_topic": {
        |    "0": 105,
        |    "1": 90
        |  }
        |}
        |""".stripMargin

    val endingOffsets = "latest"

    val kafkaDf = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", startingOffsets)
      .option("endingOffsets", endingOffsets)
      .option("subscribe", "test_topic")
      .load()

    // Extract and process data
    val parsedDf = kafkaDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

    // Data reconciliation logic (example: count records)
    val count = parsedDf.count()

    println(s"Total records between offsets: $count")

    // Optionally write to disk for reconciliation output
    parsedDf.write.mode("overwrite").json("/Users/you/output/kafka-batch-data")
  }
}
```

---

### ✅ **Summary of Options Used**

| Option                 | Purpose                                    |
| ---------------------- | ------------------------------------------ |
| `startingOffsets`      | Starting point based on timestamp lookup   |
| `endingOffsets`        | Where to stop (often `"latest"` for recon) |
| `read.format("kafka")` | Batch read of Kafka messages               |

---

### ✅ **Batch Reconciliation Use Case**

* ✅ **Snapshot reporting**
* ✅ **Cross-system reconciliation**
* ✅ **Debugging missing/misprocessed data between time ranges**

---

### 
### 
---

### ✅ **1. Meaning of “0” and “1” in Offsets JSON**

* Kafka topics are split into **partitions** (e.g., partition 0, partition 1, etc.).
* When you specify starting/ending offsets in Spark, you must specify **offset per partition**.

Example:

```json
{
  "test_topic": {
    "0": 105,
    "1": 90
  }
}
```

* `"0": 105` → Start from **offset 105** in **partition 0**.
* `"1": 90` → Start from **offset 90** in **partition 1**.

---

### ✅ **2. How to Dynamically Get Offsets From Timestamps**

Since you have timestamps (e.g., `2025-07-10 00:00:00.000`), you need to:

* Convert them to **epoch milliseconds** (Unix timestamp).
* Use Kafka APIs to **fetch offsets for timestamps** for **each partition**.

---

### ✅ **3. Practical Code Example: Using AdminClient in Scala**

You can get offsets programmatically using **Kafka AdminClient** in Scala.

#### ✅ Example Code to Convert Timestamp to Offsets:

```scala
import java.time.Instant
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, ListOffsetsResult, OffsetSpec}
import org.apache.kafka.common.TopicPartition

val topic = "test_topic"
val startingTime = "2025-07-10 00:00:00.000"
val endingTime = "2025-07-10 23:59:59.999"

// Convert to milliseconds
val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
val startingEpochMs = Instant.from(formatter.parse(startingTime)).toEpochMilli
val endingEpochMs = Instant.from(formatter.parse(endingTime)).toEpochMilli

// Setup Kafka Admin Client
val props = new Properties()
props.put("bootstrap.servers", "localhost:9092")
val admin = AdminClient.create(props)

// Get partitions
val partitions = admin.describeTopics(List(topic).asJava).all().get().get(topic).partitions().asScala.map(_.partition())

// Create TopicPartitions
val topicPartitions = partitions.map(p => new TopicPartition(topic, p))

// Create timestamp mapping
val timestampsToSearch = topicPartitions.map(tp => tp -> OffsetSpec.forTimestamp(startingEpochMs)).toMap.asJava
val endTimestampsToSearch = topicPartitions.map(tp => tp -> OffsetSpec.forTimestamp(endingEpochMs)).toMap.asJava

// Get Offsets
val startOffsets = admin.listOffsets(timestampsToSearch).all().get()
val endOffsets = admin.listOffsets(endTimestampsToSearch).all().get()

// Format JSON String for Spark
val startingOffsetsJson = partitions.map { p =>
  s""""$p": ${startOffsets.get(new TopicPartition(topic, p)).offset()}"""
}.mkString("{", ",", "}")

val endingOffsetsJson = partitions.map { p =>
  s""""$p": ${endOffsets.get(new TopicPartition(topic, p)).offset()}"""
}.mkString("{", ",", "}")

val startingOffsetsFullJson =
  s"""{"$topic": $startingOffsetsJson}"""
val endingOffsetsFullJson =
  s"""{"$topic": $endingOffsetsJson}"""

println(s"startingOffsets = $startingOffsetsFullJson")
println(s"endingOffsets = $endingOffsetsFullJson")

admin.close()
```

---

### ✅ **4. Summary of Flow**

| Step | What You Do                                                               |
| ---- | ------------------------------------------------------------------------- |
| 1️⃣  | Input human-readable timestamp (`yyyy-MM-dd HH:mm:ss.SSS`)                |
| 2️⃣  | Convert to epoch milliseconds                                             |
| 3️⃣  | Use **Kafka AdminClient** to fetch offsets for **each partition**         |
| 4️⃣  | Pass **startingOffsets** and **endingOffsets** JSON to Spark batch reader |

---

### ✅ **5. Spark Batch Read Example After That**

```scala
val kafkaDf = spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("startingOffsets", startingOffsetsFullJson)
  .option("endingOffsets", endingOffsetsFullJson)
  .option("subscribe", topic)
  .load()
```

---

### ✅ **End Result:**

✔️ **You dynamically generate starting/ending offsets based on precise timestamps**, no hardcoding partition numbers.
✔️ **Each partition is handled properly**, so reconciliation is accurate and complete.

---

If you want, I can prepare a **ready-to-run Scala object with everything end-to-end** — just let me know.
