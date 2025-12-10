from shared.utils.log_setup import setup_logging


def main():
    # Setup logging
    logger = setup_logging()
    logger.info("Ingestion service starting...")
    
    try:
        # Example logging at different levels
        logger.debug("Debug message - detailed info")
        logger.info("Info message - general info")
        logger.warning("Warning message - something to watch")
        
        # Simulate some work
        data = {"records": 100, "status": "success"}
        logger.info(f"Processed {data['records']} records", extra=data)
        
        # Test exception logging
        try:
            result = 10 / 0
        except ZeroDivisionError:
            logger.error("Division by zero occurred", exc_info=True)
        
        logger.info("Ingestion completed successfully")
        
    except Exception as e:
        logger.critical("Fatal error in ingestion service", exc_info=True)
        raise


if __name__ == "__main__":
    main()