from abc import ABC, abstractmethod
import yaml
import logging
import json
from typing import Any, Dict, List, Optional, Generator
from datetime import datetime
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import queue
from pathlib import Path
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---- Data Models ----
@dataclass
class Record:
    """Base class for data records"""
    id: str
    timestamp: datetime
    data: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Record':
        return cls(
            id=data['id'],
            timestamp=datetime.fromisoformat(data['timestamp']),
            data=data['data']
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'timestamp': self.timestamp.isoformat(),
            'data': self.data
        }

# ---- Pipeline Components ----
class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    def read(self, config: Dict[str, Any]) -> Generator[Record, None, None]:
        """Read records from the source"""
        pass

    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate source configuration"""
        pass

class DataTransformation(ABC):
    """Abstract base class for data transformations"""
    
    @abstractmethod
    def transform(self, record: Record, config: Dict[str, Any]) -> Optional[Record]:
        """Transform a single record"""
        pass

    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate transformation configuration"""
        pass

class DataSink(ABC):
    """Abstract base class for data sinks"""
    
    @abstractmethod
    def write(self, records: List[Record], config: Dict[str, Any]) -> None:
        """Write records to the sink"""
        pass

    @abstractmethod
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate sink configuration"""
        pass

# ---- Implementations ----
class CSVDataSource(DataSource):
    """Read data from CSV files"""
    
    def read(self, config: Dict[str, Any]) -> Generator[Record, None, None]:
        file_path = config['file_path']
        logger.info(f"Reading from CSV: {file_path}")
        
        df = pd.read_csv(file_path)
        for _, row in df.iterrows():
            data = row.to_dict()
            yield Record(
                id=str(data.pop('id')),
                timestamp=datetime.fromisoformat(data.pop('timestamp')),
                data=data
            )

    def validate_config(self, config: Dict[str, Any]) -> bool:
        required_fields = ['file_path']
        return all(field in config for field in required_fields)

class JSONDataSource(DataSource):
    """Read data from JSON files"""
    
    def read(self, config: Dict[str, Any]) -> Generator[Record, None, None]:
        file_path = config['file_path']
        logger.info(f"Reading from JSON: {file_path}")
        
        with open(file_path, 'r') as f:
            data = json.load(f)
            for record in data:
                yield Record.from_dict(record)

    def validate_config(self, config: Dict[str, Any]) -> bool:
        required_fields = ['file_path']
        return all(field in config for field in required_fields)

class FilterTransformation(DataTransformation):
    """Filter records based on conditions"""
    
    def transform(self, record: Record, config: Dict[str, Any]) -> Optional[Record]:
        field = config['field']
        value = config['value']
        operator = config.get('operator', 'eq')
        
        record_value = record.data.get(field)
        
        if operator == 'eq':
            return record if record_value == value else None
        elif operator == 'gt':
            return record if record_value > value else None
        elif operator == 'lt':
            return record if record_value < value else None
        elif operator == 'contains':
            return record if value in record_value else None
        else:
            raise ValueError(f"Unknown operator: {operator}")

    def validate_config(self, config: Dict[str, Any]) -> bool:
        required_fields = ['field', 'value']
        return all(field in config for field in required_fields)

class EnrichTransformation(DataTransformation):
    """Enrich records with additional data"""
    
    def transform(self, record: Record, config: Dict[str, Any]) -> Record:
        enrichments = config['enrichments']
        for field, value in enrichments.items():
            record.data[field] = value
        return record

    def validate_config(self, config: Dict[str, Any]) -> bool:
        required_fields = ['enrichments']
        return all(field in config for field in required_fields)

class CSVDataSink(DataSink):
    """Write records to CSV file"""
    
    def write(self, records: List[Record], config: Dict[str, Any]) -> None:
        file_path = config['file_path']
        logger.info(f"Writing to CSV: {file_path}")
        
        data = [record.to_dict() for record in records]
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)

    def validate_config(self, config: Dict[str, Any]) -> bool:
        required_fields = ['file_path']
        return all(field in config for field in required_fields)

class JSONDataSink(DataSink):
    """Write records to JSON file"""
    
    def write(self, records: List[Record], config: Dict[str, Any]) -> None:
        file_path = config['file_path']
        logger.info(f"Writing to JSON: {file_path}")
        
        data = [record.to_dict() for record in records]
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)

    def validate_config(self, config: Dict[str, Any]) -> bool:
        required_fields = ['file_path']
        return all(field in config for field in required_fields)

# ---- Pipeline Orchestrator ----
class Pipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config_path: str):
        self.load_config(config_path)
        self.setup_components()
        self.record_queue = queue.Queue(maxsize=1000)
        self.batch_size = self.config.get('batch_size', 100)
        self.num_workers = self.config.get('num_workers', 4)

    def load_config(self, config_path: str) -> None:
        """Load pipeline configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    def setup_components(self) -> None:
        """Initialize pipeline components"""
        component_registry = {
            # Sources
            'csv_source': CSVDataSource,
            'json_source': JSONDataSource,
            # Transformations
            'filter': FilterTransformation,
            'enrich': EnrichTransformation,
            # Sinks
            'csv_sink': CSVDataSink,
            'json_sink': JSONDataSink
        }

        # Create components
        source_type = self.config['source']['type']
        self.source = component_registry[source_type]()

        self.transformations = []
        for t_config in self.config.get('transformations', []):
            t_type = t_config['type']
            self.transformations.append(component_registry[t_type]())

        sink_type = self.config['sink']['type']
        self.sink = component_registry[sink_type]()

    def process_record(self, record: Record) -> Optional[Record]:
        """Apply transformations to a single record"""
        for i, transformation in enumerate(self.transformations):
            config = self.config['transformations'][i]
            record = transformation.transform(record, config)
            if record is None:
                return None
        return record

    def process_batch(self, batch: List[Record]) -> None:
        """Process and write a batch of records"""
        processed_records = []
        for record in batch:
            processed = self.process_record(record)
            if processed:
                processed_records.append(processed)
        
        if processed_records:
            self.sink.write(processed_records, self.config['sink'])

    def run(self) -> None:
        """Execute the pipeline"""
        try:
            logger.info("Starting pipeline execution")
            start_time = datetime.now()

            # Create thread pool for parallel processing
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                current_batch = []
                
                # Process records
                for record in self.source.read(self.config['source']):
                    current_batch.append(record)
                    
                    if len(current_batch) >= self.batch_size:
                        # Process batch in thread pool
                        batch_to_process = current_batch
                        current_batch = []
                        executor.submit(self.process_batch, batch_to_process)

                # Process final batch
                if current_batch:
                    self.process_batch(current_batch)

            execution_time = datetime.now() - start_time
            logger.info(f"Pipeline completed in {execution_time}")

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise

# ---- Example Usage ----
def main():
    # Example configuration
    config = {
        "source": {
            "type": "csv_source",
            "file_path": "input.csv"
        },
        "transformations": [
            {
                "type": "filter",
                "field": "status",
                "value": "active",
                "operator": "eq"
            },
            {
                "type": "enrich",
                "enrichments": {
                    "processed_date": datetime.now().isoformat(),
                    "pipeline_version": "1.0"
                }
            }
        ],
        "sink": {
            "type": "json_sink",
            "file_path": "output.json"
        },
        "batch_size": 100,
        "num_workers": 4
    }

    # Save config
    with open('pipeline_config.yaml', 'w') as f:
        yaml.dump(config, f)

    # Create and run pipeline
    pipeline = Pipeline('pipeline_config.yaml')
    pipeline.run()

if __name__ == "__main__":
    main()
