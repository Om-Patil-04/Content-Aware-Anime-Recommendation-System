
# Content-Aware Anime Recommendation System

A sophisticated machine learning-based anime recommendation system that leverages content-aware features and collaborative filtering to provide personalized anime recommendations to users.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Pipeline](#data-pipeline)
- [Models](#models)
- [Technologies & Dependencies](#technologies--dependencies)
- [System Architecture](#system-architecture)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This project implements a content-aware anime recommendation engine that combines user rating data with anime metadata and synopsis information to generate highly personalized recommendations. The system uses deep learning embeddings to capture both collaborative and content-based signals, enabling it to recommend anime that align with individual user preferences.

**Key Achievements:**
- Successfully trained multiple embedding-based models with different embedding dimensions (32, 64, 128)
- Achieved optimal performance with MSE loss optimization
- Processed and normalized large-scale user-rating datasets
- Implemented a robust data preprocessing pipeline with quality filtering

---

## ✨ Features

- **Content-Aware Filtering**: Incorporates anime genres and synopses to enhance recommendations
- **Collaborative Filtering**: Leverages user-anime interaction patterns through embedding layers
- **User Rating Normalization**: Intelligent normalization of ratings for better model training
- **Duplicate Detection & Cleaning**: Removes duplicate entries and invalid data points
- **ID Encoding System**: Efficient user and anime ID encoding with bidirectional mappings
- **Multiple Model Variants**: Pre-trained models with different embedding dimensions for flexibility
- **Comprehensive Logging**: Detailed logging system for debugging and monitoring
- **Cloud-Ready Data Ingestion**: Integration with Google Cloud Storage for scalable data handling

---

## 📁 Project Structure

```
Content-Aware-Anime-Recommendation-System/
│
├── config/                          # Configuration files
│   ├── __init__.py
│   ├── config.yaml                  # Main configuration (GCS bucket, file names)
│   └── paths_config.py              # Path configuration (data directories)
│
├── src/                             # Source code
│   ├── __init__.py
│   ├── logger.py                    # Logging configuration and utilities
│   ├── exceptions.py                # Custom exception handling
│   └── data_ingestion.py            # GCS data download functionality
│
├── pipeline/                        # Data processing pipeline
│   ├── __init__.py
│   └── data_preprocessing.py        # Complete preprocessing workflow
│
├── models/                          # Pre-trained model weights
│   ├── best_mse_epoch1.pth          # Best MSE model checkpoint (18.2 MB)
│   ├── embedding_size_32_model.pth  # Embedding size 32 model (3.0 MB)
│   ├── embedding_size_64_model.pth  # Embedding size 64 model (6.1 MB)
│   ├── embedding_size_128_model.pth # Embedding size 128 model (12.1 MB)
│   ├── final_checkpoint_MSE.pth     # Final MSE checkpoint (18.2 MB)
│   ├── final_model_MSE.pth          # Final MSE model (6.1 MB)
│   └── final_model_with_metrics.pth # Model with evaluation metrics (18.2 MB)
│
├── notebooks/                       # Jupyter notebooks
│   └── model_training_workflow.ipynb # Complete training pipeline notebook
│
├── utils/                           # Utility functions
│   ├── __init__.py
│   └── common_functions.py          # Common helper functions (YAML reading, etc.)
│
├── artifacts/                       # Generated data (gitignored)
│   ├── raw/                         # Raw downloaded data
│   └── processed/                   # Processed and cleaned data
│
├── Logs/                            # Application logs (gitignored)
│   └── *.log                        # Timestamped log files
│
├── requirements.txt                 # Python dependencies
├── setup.py                         # Package setup configuration
├── .gitignore                       # Git ignore rules
└── README.md                        # This file

```

---

## 🚀 Installation

### Prerequisites
- Python 3.8 or higher
- pip or conda package manager
- Google Cloud Storage credentials (for data ingestion)

### Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Om-Patil-04/Content-Aware-Anime-Recommendation-System.git
   cd Content-Aware-Anime-Recommendation-System
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up Google Cloud credentials (if using data ingestion):**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
   ```

5. **Install the package in development mode:**
   ```bash
   pip install -e .
   ```

---

## ⚙️ Configuration

### Main Configuration (config/config.yaml)

```yaml
data_ingestion:
  bucket_name: "anime_project"
  bucket_file_names:
    - "anime.csv"
    - "anime_with_synopsis.csv"
    - "animelist.csv"
```

**Configuration Parameters:**
- `bucket_name`: Google Cloud Storage bucket containing the datasets
- `bucket_file_names`: List of CSV files to download from GCS

### Paths Configuration (config/paths_config.py)

```python
RAW_DIR = "artifacts/raw"              # Raw data directory
CONFIG_PATH = "config/config.yaml"     # Configuration file path
PROCESSED_DIR = "artifacts/processed"  # Processed data output directory
```

---

## 📊 Usage

### Data Ingestion

Download data from Google Cloud Storage:

```python
from src.data_ingestion import DataIngestion
from utils.common_functions import read_yaml

config = read_yaml("config/config.yaml")
data_ingestion = DataIngestion(config)
data_ingestion.run()
```

Or run directly:
```bash
python src/data_ingestion.py
```

### Data Preprocessing

Run the complete preprocessing pipeline:

```python
from pipeline.data_preprocessing import DataPreprocessor

preprocessor = DataPreprocessor()
result = preprocessor.run_full_pipeline(
    min_ratings_per_user=400,
    test_size=1000,
    random_state=43
)
```

Or run as a script:
```bash
python pipeline/data_preprocessing.py
```

### Model Training

Use the provided Jupyter notebook for the complete training workflow:

```bash
jupyter notebook notebooks/model_training_workflow.ipynb
```

---

## 🔄 Data Pipeline

### Pipeline Overview

```
Raw Data (GCS) 
    ↓
[Data Ingestion] → artifacts/raw/
    ↓
[Data Preprocessing]
    ├── Load Data (animelist.csv, anime.csv, anime_with_synopsis.csv)
    ├── Clean Rating Data (remove duplicates, handle missing values)
    ├── Filter Users (min 400 ratings per user)
    ├── Normalize Ratings (scale to [0, 1])
    ├── Encode IDs (user & anime ID mapping)
    ├── Clean Anime Metadata (genre/synopsis extraction)
    ├── Shuffle & Split Data (train/test split)
    └── Save Processed Data → artifacts/processed/
    ↓
[Model Training]
    ├── Load processed data
    ├── Create embeddings (32, 64, 128 dimensions)
    ├── Train with MSE loss
    └── Save models → models/
    ↓
[Inference]
    └── Generate personalized recommendations
```

### Data Preprocessing Details

**1. Data Loading:**
- Loads user ratings from `animelist.csv` (columns: user_id, anime_id, rating)
- Loads anime metadata from `anime.csv` (genres, types, episodes)
- Loads synopsis data from `anime_with_synopsis.csv`

**2. Cleaning:**
- Removes duplicate entries
- Handles missing values in user_id, anime_id, rating columns
- Filters users with at least 400 ratings to ensure data quality

**3. Normalization:**
- Min-max normalization of ratings to [0, 1] range
- Original rating range documented and preserved in logs

**4. Encoding:**
- Creates bidirectional user ID mappings
- Creates bidirectional anime ID mappings
- Enables efficient lookup and reverse lookup

**5. Train-Test Split:**
- Default test set: 1000 samples
- Remaining data used for training
- Maintains random state for reproducibility

**6. Output Files (artifacts/processed/):**
- `processed_ratings.csv` - Cleaned and normalized ratings
- `processed_anime.csv` - Cleaned anime metadata
- `processed_synopsis.csv` - Synopsis and genre data
- `id_encodings.json` - ID encoding mappings

---

## 🧠 Models

### Available Pre-trained Models

| Model | Embedding Size | File Size | Description |
|-------|---|---|---|
| `embedding_size_32_model.pth` | 32 | 3.0 MB | Lightweight model for faster inference |
| `embedding_size_64_model.pth` | 64 | 6.1 MB | Balanced performance/efficiency |
| `embedding_size_128_model.pth` | 128 | 12.1 MB | High-capacity model |
| `best_mse_epoch1.pth` | - | 18.2 MB | Best validation performance |
| `final_model_MSE.pth` | 64 | 6.1 MB | Final optimized model |
| `final_checkpoint_MSE.pth` | - | 18.2 MB | Final checkpoint with training state |
| `final_model_with_metrics.pth` | - | 18.2 MB | Model with evaluation metrics |

### Model Architecture

The models use an embedding-based neural network architecture:
- **Input**: User ID and Anime ID
- **Embedding Layer**: Converts IDs to dense vectors
- **Architecture**: Concatenates user and anime embeddings
- **Output**: Predicted rating (0-1 scale)
- **Loss Function**: Mean Squared Error (MSE)

### Loading a Model

```python
import torch

# Load model
model = torch.load('models/final_model_MSE.pth')
model.eval()

# Make predictions
with torch.no_grad():
    user_id = torch.tensor([encoded_user_id])
    anime_id = torch.tensor([encoded_anime_id])
    prediction = model(user_id, anime_id)
```

---

## 📦 Technologies & Dependencies

### Core Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| numpy | Latest | Numerical computing |
| pandas | Latest | Data manipulation |
| torch | Latest | Deep learning framework |
| scikit-learn | Latest | ML utilities |
| tqdm | Latest | Progress bars |
| matplotlib | Latest | Visualization |
| seaborn | Latest | Statistical visualization |
| setuptools | Latest | Package management |

### Additional Dependencies
- `google-cloud-storage` - For GCS data ingestion
- `pyyaml` - For YAML configuration parsing

### Installation via requirements.txt

```bash
pip install -r requirements.txt
```

---

## 🏗️ System Architecture

### Module Structure

#### 1. **Configuration Layer** (`config/`)
- Centralized configuration management
- Path definitions for data directories
- GCS bucket configuration

#### 2. **Core Source Layer** (`src/`)
- **logger.py**: Timestamp-based log file generation with consistent formatting
- **exceptions.py**: Custom exception handling with detailed traceback information
- **data_ingestion.py**: GCS client integration for cloud data download

#### 3. **Data Pipeline Layer** (`pipeline/`)
- **data_preprocessing.py**: End-to-end data transformation pipeline with 7+ processing stages

#### 4. **Utilities Layer** (`utils/`)
- **common_functions.py**: Shared functions (YAML config reading, helper functions)

#### 5. **Model Storage** (`models/`)
- Pre-trained PyTorch model checkpoints
- Multiple embedding dimension variants
- Best performing models with metrics

#### 6. **Analysis & Development** (`notebooks/`)
- Jupyter notebook with complete training workflow
- Exploratory data analysis
- Model evaluation and visualization

### Data Flow

```
User Input → ID Encoding → Model Inference → Rating Prediction → Recommendation
```

---

## 📝 Logging

The system implements comprehensive logging using Python's `logging` module:

**Log Location:** `Logs/` directory (one file per execution)

**Log Format:**
```
YYYY-MM-DD HH:MM:SS - LEVEL - Message
```

**Log Files:**
```
Logs/
├── 2026-04-15_10-30-45.log
├── 2026-04-15_11-15-20.log
└── ... (timestamped by execution time)
```

**Logged Events:**
- Data ingestion progress
- Pipeline stage completion
- Data statistics and summaries
- Error details with traceback
- Configuration loading status

---

## 🔍 Custom Exception Handling

The system includes a custom exception handler that provides detailed error information:

```python
from src.exceptions import CustomException

raise CustomException("Error message", sys)
# Output: "Error in filename.py, line 123: Error message"
```

---

## 📈 Data Statistics

After preprocessing, the system logs comprehensive statistics:

```
==============================================================
DATA STATISTICS
==============================================================
Total ratings: XXXXX
Unique users: XXX
Unique animes: XXXX
Rating range: [0.0000, 1.0000]
Average rating: X.XXXX
Rating std dev: X.XXXX

Ratings per user:
  Mean: XXXX.XX, Std: XXXX.XX
  Min: XXXX, Max: XXXX

Ratings per anime:
  Mean: XXX.XX, Std: XXX.XX
  Min: XX, Max: XXXX
==============================================================
```

---

## 🎓 Example Workflow

### Complete End-to-End Example

```python
# Step 1: Load configuration
from utils.common_functions import read_yaml
config = read_yaml("config/config.yaml")

# Step 2: Ingest data from GCS
from src.data_ingestion import DataIngestion
data_ingestion = DataIngestion(config)
data_ingestion.run()

# Step 3: Preprocess data
from pipeline.data_preprocessing import DataPreprocessor
preprocessor = DataPreprocessor()
result = preprocessor.run_full_pipeline()

# Step 4: Train model (see notebooks/model_training_workflow.ipynb)
# ... model training code ...

# Step 5: Generate recommendations
import torch
model = torch.load('models/final_model_MSE.pth')

# Get top N recommendations for a user
def get_recommendations(user_id, num_recommendations=10):
    # Encode user and anime IDs
    # Generate predictions
    # Return top N highest rated anime
    pass
```

---

## 🚨 Troubleshooting

### Issue: GCS Authentication Failed
**Solution:** Ensure `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to valid credentials JSON file

### Issue: File Not Found Error
**Solution:** Check that `artifacts/raw/` and `artifacts/processed/` directories are created and accessible

### Issue: Out of Memory Error
**Solution:** Reduce embedding dimension or decrease batch size in model training

### Issue: Missing Dependencies
**Solution:** Run `pip install -r requirements.txt` and verify installations with `pip list`

---

## 📬 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📄 License

This project is available under an open-source license. See the repository for specific license details.

---

## 👨‍💻 Author

**Om Patil**
- GitHub: [@Om-Patil-04](https://github.com/Om-Patil-04)
- Repository: [Content-Aware-Anime-Recommendation-System](https://github.com/Om-Patil-04/Content-Aware-Anime-Recommendation-System)

---

## 📚 References & Resources

- **PyTorch Embeddings**: https://pytorch.org/docs/stable/nn.html#embedding
- **Collaborative Filtering**: https://en.wikipedia.org/wiki/Collaborative_filtering
- **Google Cloud Storage**: https://cloud.google.com/storage/docs
- **Data Preprocessing Best Practices**: https://scikit-learn.org/stable/modules.preprocessing.html

---

## ⭐ Support

If you found this project helpful, please consider:
- Starring the repository
- Sharing with others interested in recommendation systems
- Reporting issues or suggesting improvements

---

**Last Updated:** April 15, 2026  
**Repository ID:** 1141169452
