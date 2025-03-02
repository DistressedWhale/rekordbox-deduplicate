import logging

class ColoredFormatter(logging.Formatter):
    # ANSI escape codes for colors
    INFO_COLOR = "\033[1;32m"  # Green
    WARNING_COLOR = "\033[1;33m"  # Yellow
    ERROR_COLOR = "\033[1;31m"  # Red
    RESET_COLOR = "\033[0m"  # Reset to default

    def format(self, record):
        # Colorize the INFO messages
        if record.levelno == logging.INFO:
            record.msg = f"{self.INFO_COLOR}{record.msg}{self.RESET_COLOR}"
        elif record.levelno == logging.WARNING:
            record.msg = f"{self.WARNING_COLOR}{record.msg}{self.RESET_COLOR}"
        elif record.levelno == logging.ERROR:
            record.msg = f"{self.ERROR_COLOR}{record.msg}{self.RESET_COLOR}"

        return super().format(record)