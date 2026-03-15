import os
import pandas as pd
import numpy as np
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DIR = "artifacts/raw"
PROCESSED_DIR = "artifacts/processed"
CONFIG_PATH = "config/config.yaml"

os.makedirs(PROCESSED_DIR, exist_ok=True)

class DataPreprocessor:

    def __init__(self, raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR):
        self.raw_dir = raw_dir
        self.processed_dir = processed_dir
        self.rating_df = None
        self.anime_df = None
        self.synopsis_df = None
        
    def load_raw_data(self):
        logger.info(f"Loading raw data from {self.raw_dir}...")
        
        try:
            self.rating_df = pd.read_csv(
                os.path.join(self.raw_dir, "animelist.csv"),
                low_memory=True,
                usecols=["user_id", "anime_id", "rating"]
            )
            logger.info(f"Loaded ratings: {len(self.rating_df)} records")
            
            self.anime_df = pd.read_csv(
                os.path.join(self.raw_dir, "anime.csv"),
                low_memory=True
            )
            logger.info(f"Loaded anime metadata: {len(self.anime_df)} records")
            
            self.synopsis_df = pd.read_csv(
                os.path.join(self.raw_dir, "anime_with_synopsis.csv"),
                usecols=["MAL_ID", "Name", "Genres", "sypnopsis"]
            )
            logger.info(f"Loaded synopsis data: {len(self.synopsis_df)} records")
            
            return True
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
            return False
    
    def clean_rating_data(self, min_ratings_per_user=400):
        logger.info("Cleaning rating data...")
        
        duplicates = self.rating_df.duplicated().sum()
        logger.info(f"Duplicate entries: {duplicates}")
        if duplicates > 0:
            self.rating_df = self.rating_df.drop_duplicates()
        
        null_counts = self.rating_df.isnull().sum()
        logger.info(f"Missing values:\n{null_counts}")
        
        self.rating_df = self.rating_df.dropna(subset=['user_id', 'anime_id', 'rating'])
        
        user_counts = self.rating_df["user_id"].value_counts()
        min_users = user_counts[user_counts >= min_ratings_per_user].index
        self.rating_df = self.rating_df[self.rating_df["user_id"].isin(min_users)].copy()
        
        logger.info(f"After filtering: {len(self.rating_df)} records, {len(self.rating_df['user_id'].unique())} users")
        
        return self.rating_df
    
    def normalize_ratings(self):
        logger.info("Normalizing ratings...")
        
        min_rating = self.rating_df["rating"].min()
        max_rating = self.rating_df["rating"].max()
        
        logger.info(f"Original rating range: [{min_rating}, {max_rating}]")
        
        self.rating_df["rating"] = self.rating_df["rating"].apply(
            lambda x: (x - min_rating) / (max_rating - min_rating)
        ).astype(np.float64)
        
        logger.info(f"Normalized rating range: [{self.rating_df['rating'].min()}, {self.rating_df['rating'].max()}]")
        
        return self.rating_df
    
    def encode_ids(self):
        logger.info("Encoding user and anime IDs...")
        
        user_ids = self.rating_df["user_id"].unique().tolist()
        self.user2user_encoded = {x: i for i, x in enumerate(user_ids)}
        self.user2user_decoded = {i: x for i, x in enumerate(user_ids)}
        self.rating_df["user"] = self.rating_df["user_id"].map(self.user2user_encoded)
        
        anime_ids = self.rating_df["anime_id"].unique().tolist()
        self.anime2anime_encoded = {x: i for i, x in enumerate(anime_ids)}
        self.anime2anime_decoded = {i: x for i, x in enumerate(anime_ids)}
        self.rating_df["anime"] = self.rating_df["anime_id"].map(self.anime2anime_encoded)
        
        logger.info(f"Encoded {len(self.user2user_encoded)} users and {len(self.anime2anime_encoded)} animes")
        
        return {
            'user2user_encoded': self.user2user_encoded,
            'user2user_decoded': self.user2user_decoded,
            'anime2anime_encoded': self.anime2anime_encoded,
            'anime2anime_decoded': self.anime2anime_decoded
        }
    
    def clean_anime_metadata(self):
        logger.info("Cleaning anime metadata...")
        
        self.anime_df = self.anime_df.replace("Unknown", np.nan)
        
        if 'MAL_ID' in self.anime_df.columns:
            self.anime_df['anime_id'] = self.anime_df['MAL_ID']
        if 'English name' in self.anime_df.columns:
            self.anime_df['eng_version'] = self.anime_df['English name']
        
        logger.info(f"Cleaned anime metadata: {len(self.anime_df)} records")
        
        return self.anime_df
    
    def shuffle_and_split_data(self, random_state=43):
        logger.info("Shuffling and splitting data...")
        
        self.rating_df = self.rating_df.sample(frac=1, random_state=random_state).reset_index(drop=True)
        
        logger.info(f"Data shuffled with random_state={random_state}")
        
        return self.rating_df
    
    def prepare_train_test_split(self, test_size=1000):
        logger.info(f"Preparing train/test split (test_size={test_size})...")
        
        train_indices = self.rating_df.shape[0] - test_size
        
        X = self.rating_df[["user", "anime"]].values
        y = self.rating_df["rating"].values
        
        X_train = X[:train_indices]
        X_test = X[train_indices:]
        y_train = y[:train_indices]
        y_test = y[train_indices:]
        
        logger.info(f"Train samples: {len(X_train)}, Test samples: {len(X_test)}")
        
        return {
            'X_train': X_train,
            'X_test': X_test,
            'y_train': y_train,
            'y_test': y_test
        }
    
    def save_processed_data(self):
        logger.info(f"Saving processed data to {self.processed_dir}...")
        
        self.rating_df.to_csv(
            os.path.join(self.processed_dir, "processed_ratings.csv"),
            index=False
        )
        logger.info("Saved processed_ratings.csv")
        
        self.anime_df.to_csv(
            os.path.join(self.processed_dir, "processed_anime.csv"),
            index=False
        )
        logger.info("Saved processed_anime.csv")
        
        self.synopsis_df.to_csv(
            os.path.join(self.processed_dir, "processed_synopsis.csv"),
            index=False
        )
        logger.info("Saved processed_synopsis.csv")
        
        import json
        
        encodings = {
            'user2user_encoded': self.user2user_encoded,
            'user2user_decoded': {str(k): v for k, v in self.user2user_decoded.items()},
            'anime2anime_encoded': self.anime2anime_encoded,
            'anime2anime_decoded': {str(k): v for k, v in self.anime2anime_decoded.items()}
        }
        
        with open(os.path.join(self.processed_dir, "id_encodings.json"), 'w') as f:
            json.dump(encodings, f)
        logger.info("Saved id_encodings.json")
        
        return True
    
    def generate_statistics(self):
        logger.info("\n" + "="*60)
        logger.info("DATA STATISTICS")
        logger.info("="*60)
        
        logger.info(f"Total ratings: {len(self.rating_df)}")
        logger.info(f"Unique users: {self.rating_df['user_id'].nunique()}")
        logger.info(f"Unique animes: {self.rating_df['anime_id'].nunique()}")
        logger.info(f"Rating range: [{self.rating_df['rating'].min():.4f}, {self.rating_df['rating'].max():.4f}]")
        logger.info(f"Average rating: {self.rating_df['rating'].mean():.4f}")
        logger.info(f"Rating std dev: {self.rating_df['rating'].std():.4f}")
        
        ratings_per_user = self.rating_df.groupby('user_id').size()
        logger.info(f"Ratings per user - Mean: {ratings_per_user.mean():.2f}, Std: {ratings_per_user.std():.2f}")
        logger.info(f"Ratings per user - Min: {ratings_per_user.min()}, Max: {ratings_per_user.max()}")
        
        ratings_per_anime = self.rating_df.groupby('anime_id').size()
        logger.info(f"Ratings per anime - Mean: {ratings_per_anime.mean():.2f}, Std: {ratings_per_anime.std():.2f}")
        logger.info(f"Ratings per anime - Min: {ratings_per_anime.min()}, Max: {ratings_per_anime.max()}")
        
        logger.info("="*60 + "\n")
    
    def run_full_pipeline(self, min_ratings_per_user=400, test_size=1000, random_state=43):
        logger.info("Starting full preprocessing pipeline...\n")
        
        if not self.load_raw_data():
            logger.error("Failed to load raw data")
            return False
        
        self.clean_rating_data(min_ratings_per_user=min_ratings_per_user)
        self.normalize_ratings()
        self.encode_ids()
        self.clean_anime_metadata()
        self.shuffle_and_split_data(random_state=random_state)
        
        self.generate_statistics()
        
        self.save_processed_data()
        
        logger.info("Pipeline completed successfully!")
        
        return {
            'rating_df': self.rating_df,
            'anime_df': self.anime_df,
            'synopsis_df': self.synopsis_df,
            'encodings': {
                'user2user_encoded': self.user2user_encoded,
                'user2user_decoded': self.user2user_decoded,
                'anime2anime_encoded': self.anime2anime_encoded,
                'anime2anime_decoded': self.anime2anime_decoded
            },
            'split': self.prepare_train_test_split(test_size=test_size)
        }


def main():
    preprocessor = DataPreprocessor(raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR)
    result = preprocessor.run_full_pipeline(
        min_ratings_per_user=400,
        test_size=1000,
        random_state=43
    )
    
    if result:
        print("\nPreprocessing completed successfully.")
        print(f"Processed data saved to: {PROCESSED_DIR}")
    else:
        print("Preprocessing failed.")


if __name__ == "__main__":
    main()