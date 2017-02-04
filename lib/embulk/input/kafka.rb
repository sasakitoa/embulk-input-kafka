Embulk::JavaPlugin.register_input(
  "kafka", "org.embulk.input.kafka.KafkaInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
