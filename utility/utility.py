import logging


def setup_logging(log_file_name):
    # Define the format for log messages
    logFormatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
    )

    # Create the root logger
    rootLogger = logging.getLogger()
    rootLogger.setLevel(logging.INFO)

    # File handler for writing logs to a file
    fileHandler = logging.FileHandler(log_file_name)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    # Console handler for printing logs to the terminal
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    return rootLogger


def format_time(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"
