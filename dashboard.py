import json
import os
import time
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentDashboard:
    def __init__(self, data_dir="output/tweets_sentiment"):
        self.data_dir = data_dir
        self.sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
        self.time_series_data = []
        self.processed_files = set()
        
        # Setup the plot
        plt.style.use('ggplot')
        self.fig, (self.ax1, self.ax2) = plt.subplots(1, 2, figsize=(15, 6))
        self.fig.suptitle('Real-time Twitter Sentiment Analysis', fontsize=16)

    def read_new_data(self):
        """Read any new data files from the output directory"""
        try:
            if not os.path.exists(self.data_dir):
                logger.warning(f"Data directory {self.data_dir} does not exist yet")
                return
            
            files = [f for f in os.listdir(self.data_dir) if f.endswith('.json')]
            new_files = [f for f in files if f not in self.processed_files]
            
            for file in new_files:
                file_path = os.path.join(self.data_dir, file)
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        self.sentiment_counts[data['sentiment']] += 1
                        
                        # Add to time series with timestamp
                        self.time_series_data.append({
                            'timestamp': data['created_at'],
                            'sentiment': data['sentiment']
                        })
                    
                    self.processed_files.add(file)
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse JSON from {file_path}")
                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
            
            # Keep only the last 100 entries for time series
            if len(self.time_series_data) > 100:
                self.time_series_data = self.time_series_data[-100:]
                
            logger.info(f"Processed {len(new_files)} new files. Total counts: {self.sentiment_counts}")
            
        except Exception as e:
            logger.error(f"Error reading new data: {e}")
    
    def update_plot(self, frame):
        """Update function for the animation"""
        self.read_new_data()
        
        # Clear the axes for new plots
        self.ax1.clear()
        self.ax2.clear()
        
        # Plot 1: Pie chart of sentiment distribution
        labels = list(self.sentiment_counts.keys())
        sizes = list(self.sentiment_counts.values())
        
        if sum(sizes) > 0:  # Only create pie chart if we have data
            self.ax1.pie(sizes, labels=labels, autopct='%1.1f%%', 
                         shadow=True, startangle=90, 
                         colors=['green', 'red', 'gray'])
            self.ax1.axis('equal')
            self.ax1.set_title('Overall Sentiment Distribution')
        else:
            self.ax1.text(0.5, 0.5, 'Waiting for data...', 
                         horizontalalignment='center', verticalalignment='center')
        
        # Plot 2: Time series of sentiment over time
        if self.time_series_data:
            # Convert to DataFrame for easier plotting
            df = pd.DataFrame(self.time_series_data)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            
            # Count sentiments in rolling window
            sentiment_counts = df.groupby(pd.Grouper(freq='1min'))['sentiment'].value_counts().unstack().fillna(0)
            
            # Plot time series
            if not sentiment_counts.empty:
                sentiment_counts.plot(ax=self.ax2, kind='line', marker='o')
                self.ax2.set_title('Sentiment Trend (Last Few Minutes)')
                self.ax2.set_xlabel('Time')
                self.ax2.set_ylabel('Count')
                self.ax2.legend(title='Sentiment')
            else:
                self.ax2.text(0.5, 0.5, 'Collecting time series data...', 
                             horizontalalignment='center', verticalalignment='center')
        else:
            self.ax2.text(0.5, 0.5, 'Waiting for data...', 
                         horizontalalignment='center', verticalalignment='center')
            
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        
        return self.ax1, self.ax2

def main():
    logger.info("Starting sentiment analysis dashboard")
    
    # Create dashboard
    dashboard = SentimentDashboard()
    
    # Set up animation
    ani = FuncAnimation(dashboard.fig, dashboard.update_plot, interval=2000)
    
    # Show the plot
    plt.show()

if __name__ == "__main__":
    print("Starting dashboard server...")
    main()  # Run the matplotlib dashboard