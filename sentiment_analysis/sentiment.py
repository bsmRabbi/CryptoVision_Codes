import pandas as pd
from transformers import pipeline

# Initialize the sentiment analysis pipeline with the DistilBERT model
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

# Load the dataset
df = pd.read_csv(r"C:\Users\mashr\OneDrive\Desktop\Files\Book1.csv")  # Use your actual file path

# Step 1: Perform sentiment analysis on the 'Filtered_Text' column
def get_sentiment_for_text(text):
    if isinstance(text, str):  # Ensure the text is a valid string
        # Tokenize and truncate the input text to a maximum length of 512 tokens
        result = sentiment_pipeline(text[:512])[0]  # Truncate text if it's too long
        label = result['label']
        score = result['score']
        
        # Post-process to add "neutral" if score is close to 0.5 for both positive and negative
        if label == 'LABEL_1' and score > 0.4:  # This assumes the model is binary (positive/negative)
            label = 'neutral'  # Neutral if the score is around 0.5
        
        return label, score
    else:
        return 'unknown', 0.0  # Return 'unknown' for invalid or NaN values with a score of 0.0

# Apply the sentiment analysis function to the 'Filtered_Text' column
df[['sentiment_label', 'sentiment_score']] = df['Filtered_Text'].apply(lambda x: pd.Series(get_sentiment_for_text(x)))

# Save the result to a new CSV file
df.to_csv("C:/Users/mashr/OneDrive/Desktop/market_data_with_sentiment.csv", index=False)

# Display the first few rows of the resulting DataFrame
print(df.head())