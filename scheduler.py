import time
import schedule

from pipeline import run_pipeline_for_cities

CITIES = ["Bhubaneswar", "Delhi", "Mumbai", "Bengaluru"]


def job():
    print("\n⏳ Running hourly pipeline...\n")
    run_pipeline_for_cities(CITIES)
    print("\n✅ Hourly pipeline cycle complete.\n")


schedule.every().hour.do(job)

# Run once immediately
job()

while True:
    schedule.run_pending()
    time.sleep(60)