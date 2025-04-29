from ping3 import ping
import csv
from datetime import datetime
import time
import signal
import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter


class IPMonitor:
    def __init__(self, ip_address, output_file="ip_monitor_results.csv"):
        self.ip_address = ip_address
        self.output_file = output_file
        self.running = True
        self.interval = 30  # Set interval to 30 seconds

        # Initialize CSV file with headers
        with open(self.output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Timestamp', 'IP', 'Status', 'Response Time (ms)'])

        # Set up signal handler for Ctrl+C
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\nStopping IP monitoring...")
        self.running = False
        self.create_chart()

    def check_ip(self):
        try:
            response_time = ping(self.ip_address, timeout=2)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if response_time is not None:
                status = "Online"
                response_ms = round(response_time * 1000, 2)
                print(f"[{timestamp}] IP {self.ip_address} is online! Response time: {response_ms}ms")
            else:
                status = "Offline"
                response_ms = None
                print(f"[{timestamp}] IP {self.ip_address} is offline or not responding")

            # Save results to CSV
            with open(self.output_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([timestamp, self.ip_address, status, response_ms])

        except Exception as e:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] Error while pinging {self.ip_address}: {str(e)}")

            # Save error to CSV
            with open(self.output_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([timestamp, self.ip_address, "Error", str(e)])

    def create_chart(self):
        print("\nCreating monitoring results chart...")
        try:
            # Read the CSV file
            df = pd.read_csv(self.output_file)

            # Convert timestamp to datetime
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])

            # Create the plot
            plt.figure(figsize=(12, 6))

            # Plot response times
            plt.plot(df['Timestamp'], df['Response Time (ms)'], 'b-', label='Response Time')

            # Mark offline points
            offline_points = df[df['Status'] == 'Offline']['Timestamp']
            if not offline_points.empty:
                plt.scatter(offline_points, [0] * len(offline_points), color='red',
                            marker='x', s=100, label='Offline')

            # Customize the plot
            plt.title(f'Ping Response Times for {self.ip_address}')
            plt.xlabel('Time')
            plt.ylabel('Response Time (ms)')
            plt.grid(True)
            plt.legend()

            # Format x-axis
            plt.gcf().autofmt_xdate()
            plt.gca().xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))

            # Save the plot
            chart_file = f'ping_monitor_chart_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png'
            plt.savefig(chart_file)
            print(f"Chart saved as {chart_file}")

        except Exception as e:
            print(f"Error creating chart: {str(e)}")

    def start_monitoring(self):
        print(f"Starting IP monitoring for {self.ip_address}")
        print(f"Ping interval: {self.interval} seconds")
        print("Press Ctrl+C to stop monitoring and generate chart")
        print(f"Results are being saved to {self.output_file}")

        while self.running:
            self.check_ip()
            time.sleep(self.interval)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        ip_to_monitor = sys.argv[1]
    else:
        ip_to_monitor = input("Enter IP address to monitor: ")

    monitor = IPMonitor(ip_to_monitor)
    monitor.start_monitoring()