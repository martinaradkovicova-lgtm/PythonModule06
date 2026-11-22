from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.items: list[str] = []
        self.rank: int = 0

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
        else:
            self.items.append(str(data))


class TextProcessor(DataProcessor):
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
        else:
            self.items.append(data)


class LogProcessor(DataProcessor):
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
        else:
            self.items.append(f"{data['log_level']}: {data['log_message']}")


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    num_p = NumericProcessor()
    print("\nTesting Numeric Processor...")
    print(f"Trying to validate input '42': {num_p.validate(42)}")
    print(f"Trying to validate 'Hello': {num_p.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_p.ingest("foo")
    except ValueError as ERROR:
        print(f"Got exception: {ERROR}")

    numbers = [1, 2, 3, 4, 5]
    print(f"Processing data: {numbers}")
    num_p.ingest(numbers)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num_p.output()
        print(f"Numeric value {rank}: {val}")

    text_p = TextProcessor()
    print("\nTesting Text Processor...")
    print(f"Trying to validate input '42': {text_p.validate(42)}")
    texts = ["Hello", "Nexus", "World"]
    print(f"Processing data: {texts}")
    text_p.ingest(texts)
    print("Extracting 1 value...")
    rank, val = text_p.output()
    print(f"Text value {rank}: {val}")

    log_p = LogProcessor()
    print("\nTesting Log Processor...")
    print(f"Trying to validate input 'Hello': {log_p.validate('Hello')}")
    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f"Processing data: {logs}")
    log_p.ingest(logs)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log_p.output()
        print(f"Log entry {rank}: {val}")


if __name__ == "__main__":
    main()
