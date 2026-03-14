#### Overview
This project focuses on building a complete real-time streaming pipeline. It begins by setting up a message broker with Redpanda (Kafka-compatible), which acts as a central system for transmitting messages between components. Python producers generate and send data (in this case, NYC taxi trip events) to the broker, while Python consumers read and retrieve these messages for further processing. The data is then processed in real time using Apache Flink and finally stored in PostgreSQL for analysis. The module provides a practical demonstration of end-to-end streaming concepts and the integration of modern open-source streaming tools.

#### Data flow example
```
[NYC Taxi Trip Data Source] 
          │
          ▼
     [Python Producer]
          │ sends data
          ▼
     [Redpanda Broker]
          │ stores & streams messages
          ▼
     [Python Consumer / Flink Processor]
          │ processes data
          ▼
     [PostgreSQL Database]
          │ stores processed results
          ▼
  [Analytics / Dashboards / Queries]
```

#### Workshop
https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/07-streaming/workshop