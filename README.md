# dpf
Data Product Framework

flexible, config-driven data pipeline framework. Here's how it works:

1. **Core Components**:
   - `DataSource`: Reads data from various sources
   - `DataTransformation`: Processes and transforms the data
   - `DataSink`: Writes the processed data to a destination
   - `Pipeline`: Orchestrates the entire process

2. **Key Features**:
   - Configuration-driven: All parameters defined in YAML
   - Extensible: Easy to add new sources, transformations, and sinks
   - Logging: Built-in logging for monitoring and debugging
   - Error handling: Proper exception handling and logging
   - Type hints: For better code maintainability

To use this framework, you would:
1. Create a YAML config file defining your pipeline
2. Implement any custom sources, transformations, or sinks
3. Instantiate and run the pipeline
