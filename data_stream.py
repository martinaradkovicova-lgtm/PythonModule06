from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    name: str = "Data Processor"

    def __init__(self) -> None:
        self.items: list[str] = []
        self.rank: int = 0
        self.total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.items:
            raise IndexError("No data to output")
        value = self.items.pop(0)
        current_rank = self.rank
        self.rank += 1
        return (current_rank, value)


class NumericProcessor(DataProcessor):
    name: str = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if (isinstance(data, (int, float))) and (not isinstance(data, bool)):
            return True

        if (isinstance(data, list)):
            if (not data):  # Empty list
                return False
            for num in data:
                if not isinstance(num, (int, float)) or isinstance(num, bool):
                    return False
            return True

        return False

    def ingest(self, data: int | float | list[int] |
               list[float] | list[int | float]) -> None:
        if (not self.validate(data)):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for num in data:
                self.items.append(str(num))
                self.total_processed += 1
        else:
            self.items.append(str(data))
            self.total_processed += 1


class TextProcessor(DataProcessor):
    name: str = "Text Processor"

    def validate(self, data: Any) -> bool:
        if (isinstance(data, str)):
            return True

        if (isinstance(data, list)):
            if (not data):  # Empty list
                return False
            for text in data:
                if (not isinstance(text, str)):
                    return False
            return True

        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for text in data:
                self.items.append(text)
                self.total_processed += 1
        else:
            self.items.append(data)
            self.total_processed += 1


class LogProcessor(DataProcessor):
    name: str = "Log Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            if ("log_level" not in data) or ("log_message" not in data):
                return False
            if (not isinstance(data["log_level"], str)):
                return False
            if (not isinstance(data["log_message"], str)):
                return False

            return True

        if isinstance(data, list):
            if not data:
                return False
            for item in data:
                if not isinstance(item, dict):
                    return False
                if ("log_level" not in item) or ("log_message" not in item):
                    return False
                if (not isinstance(item["log_level"], str)):
                    return False
                if (not isinstance(item["log_message"], str)):
                    return False
            return True

        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, list):
            for item in data:
                log_item: str = f"{item['log_level']}: {item['log_message']}"
                self.items.append(log_item)
                self.total_processed += 1
        else:
            self.items.append(f"{data['log_level']}: {data['log_message']}")
            self.total_processed += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    processed = True
                    break
            if not processed:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{element}"
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            print(
                f"{proc.name}: total {proc.total_processed} items processed, "
                f"remaining {len(proc.items)} on processor"
            )


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    data_stream: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected",
            },
        ],
        42,
        ["Hi", "five"]
    ]

    print(f"Send first batch of data on stream: {data_stream}")
    stream.process_stream(data_stream)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream.register_processor(text_proc)
    stream.register_processor(log_proc)

    print("Send the same data_stream again")
    stream.process_stream(data_stream)
    stream.print_processors_stats()

    print(
        "\nConsume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    log_proc.output()

    stream.print_processors_stats()


if __name__ == "__main__":
    main()
