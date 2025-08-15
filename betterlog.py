import logging
import sys
from pathlib import Path
from datetime import datetime
import colorlog


def setup_enhanced_logging(
        level: str = "INFO",
        log_file: str = None,
        show_function: bool = True,
        show_line: bool = True,
        colorize: bool = True,
        max_function_width: int = 20,
        log_writemode: str = "a"
):
    """
    Setup enhanced logging with function names, line numbers, and optional file output.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to log to (None = console only)
        show_function: Include function name in logs
        show_line: Include line number in logs
        colorize: Use colors in console output (requires colorlog)
        max_function_width: Maximum width for function name display

    Example:
        setup_enhanced_logging(
            level="DEBUG",
            log_file="app.log",
            show_function=True,
            show_line=True,
            colorize=True
        )
    """

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    # Build format string based on options
    format_parts = []

    # Timestamp
    format_parts.append("%(asctime)s")

    # Level with fixed width
    if colorize:
        format_parts.append("%(log_color)s%(levelname)-8s%(reset)s")
    else:
        format_parts.append("%(levelname)-8s")

    # Module name (shortened)
    format_parts.append("%(name)-15s")

    # Function and line info
    location_parts = []
    if show_function:
        location_parts.append(f"%(funcName)-{max_function_width}s")
    if show_line:
        location_parts.append("%(lineno)4d")

    if location_parts:
        location_format = "[" + ":".join(location_parts) + "]"
        format_parts.append(location_format)

    # Message
    if colorize:
        format_parts.append("%(log_color)s%(message)s%(reset)s")
    else:
        format_parts.append("%(message)s")

    # Join format parts
    log_format = " │ ".join(format_parts)

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)

    if colorize:
        try:
            console_formatter = colorlog.ColoredFormatter(
                log_format,
                datefmt='%H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                },
                secondary_log_colors={},
                style='%'
            )
        except ImportError:
            # Fallback if colorlog not available
            colorize = False
            console_formatter = logging.Formatter(
                log_format.replace('%(log_color)s', '').replace('%(reset)s', ''),
                datefmt='%H:%M:%S'
            )
    else:
        console_formatter = logging.Formatter(log_format, datefmt='%H:%M:%S')

    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        # Create directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file, mode=log_writemode)

        # File format without colors
        file_format = log_format.replace('%(log_color)s', '').replace('%(reset)s', '')
        file_formatter = logging.Formatter(
            file_format,
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Show setup info
    logger = logging.getLogger(__name__)
    logger.info(f"Enhanced logging configured - Level: {level}")
    if log_file:
        logger.info(f"Logging to file: {log_file}")
    if not colorize and 'colorlog' in log_format:
        logger.warning("colorlog not available, using plain formatting")


def setup_file_rotation_logging(
        level: str = "INFO",
        log_file: str = "app.log",
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        show_function: bool = True,
        show_line: bool = True,
        colorize_console: bool = True
):
    """
    Setup logging with automatic file rotation.

    Args:
        level: Logging level
        log_file: Base log file name
        max_bytes: Maximum size before rotation (default 10MB)
        backup_count: Number of backup files to keep
        show_function: Include function name in logs
        show_line: Include line number in logs
        colorize_console: Use colors for console output
    """
    from logging.handlers import RotatingFileHandler

    # Clear any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set logging level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    # Build format strings
    location_info = ""
    if show_function and show_line:
        location_info = " [%(funcName)-20s:%(lineno)4d]"
    elif show_function:
        location_info = " [%(funcName)-20s]"
    elif show_line:
        location_info = " [line %(lineno)4d]"

    console_format = f"%(asctime)s │ %(levelname)-8s │ %(name)-15s{location_info} │ %(message)s"
    file_format = f"%(asctime)s │ %(levelname)-8s │ %(name)-15s{location_info} │ %(message)s"

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)

    if colorize_console:
        try:
            console_formatter = colorlog.ColoredFormatter(
                f"%(asctime)s │ %(log_color)s%(levelname)-8s%(reset)s │ %(name)-15s{location_info} │ %(log_color)s%(message)s%(reset)s",
                datefmt='%H:%M:%S',
                log_colors={
                    'DEBUG': 'cyan',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'red,bg_white',
                }
            )
        except ImportError:
            console_formatter = logging.Formatter(console_format, datefmt='%H:%M:%S')
    else:
        console_formatter = logging.Formatter(console_format, datefmt='%H:%M:%S')

    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Rotating file handler
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count
    )

    file_formatter = logging.Formatter(file_format, datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    logger = logging.getLogger(__name__)
    logger.info(
        f"Rotating file logging configured - Max size: {max_bytes / 1024 / 1024:.1f}MB, Backups: {backup_count}")


def setup_structured_logging(
        level: str = "INFO",
        log_file: str = None,
        json_format: bool = False,
        include_extra_fields: bool = True
):
    """
    Setup structured logging with additional context fields.

    Args:
        level: Logging level
        log_file: Optional log file
        json_format: Output in JSON format
        include_extra_fields: Include extra context fields
    """
    import json
    from datetime import datetime

    class StructuredFormatter(logging.Formatter):
        def format(self, record):
            if json_format:
                log_entry = {
                    'timestamp': datetime.fromtimestamp(record.created).isoformat(),
                    'level': record.levelname,
                    'logger': record.name,
                    'message': record.getMessage(),
                }

                if include_extra_fields:
                    log_entry.update({
                        'module': record.module,
                        'function': record.funcName,
                        'line': record.lineno,
                        'thread': record.thread,
                        'process': record.process,
                    })

                # Add any extra fields from the log record
                for key, value in record.__dict__.items():
                    if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
                                   'module', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                                   'thread', 'threadName', 'processName', 'process', 'getMessage', 'stack_info']:
                        log_entry[key] = value

                return json.dumps(log_entry, default=str)
            else:
                # Pretty formatted structured output
                timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S.%f')[:-3]
                location = f"{record.funcName}:{record.lineno}"

                return (f"{timestamp} │ "
                        f"{record.levelname:<8} │ "
                        f"{record.name:<15} │ "
                        f"{location:<25} │ "
                        f"{record.getMessage()}")

    # Clear existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(StructuredFormatter())
    root_logger.addHandler(console_handler)

    # File handler if specified
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(StructuredFormatter())
        root_logger.addHandler(file_handler)


# Example usage and testing
if __name__ == "__main__":
    print("=== Testing Enhanced Logging Configurations ===\n")

    # Test 1: Basic enhanced logging
    print("1. Basic Enhanced Logging:")
    setup_enhanced_logging(level="DEBUG", log_file="logs/new.log", show_function=True, show_line=True)

    logger = logging.getLogger("test_service")
    logger.debug("This is a debug message")
    logger.info("Service started successfully")
    logger.warning("This is a warning message")
    logger.error("An error occurred")

    print("\n" + "=" * 80 + "\n")

    # Test 2: File rotation logging
    print("2. File Rotation Logging:")
    setup_file_rotation_logging(
        level="INFO",
        log_file="logs/app.log",
        max_bytes=1024 * 1024,  # 1MB for testing
        backup_count=3
    )

    logger = logging.getLogger("file_service")
    logger.info("This will go to both console and rotating file")
    logger.warning("Check the logs/ directory for the file output")

    print("\n" + "=" * 80 + "\n")

    # Test 3: Structured logging
    print("3. Structured Logging (Pretty Format):")
    setup_structured_logging(level="INFO", json_format=False)

    logger = logging.getLogger("structured_service")
    logger.info("Structured logging with pretty format")
    logger.error("Error with additional context", extra={'user_id': 12345, 'action': 'login'})

    print("\n" + "=" * 80 + "\n")

    # Test 4: JSON structured logging
    print("4. JSON Structured Logging:")
    setup_structured_logging(level="INFO", json_format=True, log_file="logs/structured.log")

    logger = logging.getLogger("json_service")
    logger.info("This is JSON formatted", extra={'request_id': 'abc123', 'user_agent': 'Python/3.9'})
    logger.warning("JSON warning with context", extra={'endpoint': '/api/users', 'status_code': 429})

    print("Check logs/structured.log for JSON formatted output")


# Quick setup functions for common use cases
def quick_setup_dev_logging():
    """Quick setup for development - colorful, detailed console logging"""
    setup_enhanced_logging(
        level="DEBUG",
        show_function=True,
        show_line=True,
        colorize=True
    )


def quick_setup_production_logging(log_file="logs/app.log"):
    """Quick setup for production - file rotation with structured format"""
    setup_file_rotation_logging(
        level="INFO",
        log_file=log_file,
        max_bytes=50 * 1024 * 1024,  # 50MB
        backup_count=10,
        show_function=True,
        show_line=False,
        colorize_console=False
    )


def quick_setup_json_logging(log_file="logs/app.json"):
    """Quick setup for JSON logging - great for log aggregation systems"""
    setup_structured_logging(
        level="INFO",
        log_file=log_file,
        json_format=True,
        include_extra_fields=True
    )