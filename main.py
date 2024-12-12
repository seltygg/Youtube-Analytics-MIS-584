import subprocess
import os

def run_webscraper():
    print("Starting the web scraper...")
    scraper_path = os.path.join("webscraper", "Youtube_API_Scrapper.py")
    subprocess.run(["python", scraper_path], check=True)
    print("Web scraping completed.\n")

def run_producer():
    print("Starting the Kafka producer...")
    producer_path = os.path.join("kafka", "producer.py")
    subprocess.run(["python", producer_path], check=True)
    print("Producer is streaming data.\n")

def run_consumer():
    print("Starting the Kafka consumer (web app)...")
    consumer_path = os.path.join("kafka", "consumer.py")
    subprocess.run(["python", consumer_path], check=True)
    print("Consumer is running.\n")

if __name__ == "__main__":
    try:
        # Step 1: Run web scraper
        run_webscraper()

        # Step 2: Run producer
        run_producer()

        # Step 3: Run consumer
        run_consumer()

    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
