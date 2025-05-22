This project is inspired from [Kafka Please](https://github.com/eirslett/kafka-please.git) 

## Execution

1. Clone the repository
   ```sh
   git clone https://github.com/sumant-rana/embedded-kafka.git
   cd embedded-kafka
   ```
2. Download and extract Kafka
   ```sh
   ./install_kafka.sh
   ```
2. Install the dependencies
   ```sh
   npm install
   npm i jest-cli -g
   ```
3. Export environment variable to avoind showing some warning messages related to newer version of kafkajs
   ```sh
   export KAFKAJS_NO_PARTITIONER_WARNING=1
   ```
4. Run the test
   ```sh
   jest
   ```
   The output will contain an error `The group coordinator is not available`. This has been documented here [https://github.com/tulios/kafkajs/issues/1494](https://github.com/tulios/kafkajs/issues/1494). Ignore the error
   The test should pass.
   
