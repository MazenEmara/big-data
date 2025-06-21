Based on the structure of your repo and the contents of the notebooks and scripts you've provided, here's a well-documented and professional `README.md` you can use:

---

# Big Data Projects – University Portfolio (2023)

This repository showcases a series of academic big data projects conducted in 2023. These projects utilize a range of technologies including Apache Spark, Cassandra, MongoDB, and Python data processing to analyze and visualize large datasets across multiple domains.

## 📁 Projects Overview

### 🏨 `Hotels_Project/`

A data exploration and analysis project focused on hotel bookings.

- **`m1.ipynb`**: Performs initial cleaning, exploratory data analysis (EDA), and feature engineering on hotel booking data. This includes visualizing booking trends, cancellations, and customer preferences.
- **`Project.ipynb`**: Full end-to-end analysis of hotel data. This notebook addresses questions such as:

  - What factors influence booking cancellations?
  - Which time periods have the highest demand?
  - Are there differences in behavior between city hotels and resort hotels?

### 📦 `Mini_Project1/`

A data pipeline and aggregation project using **MongoDB** and **PyMongo**, focused on NYC taxi trip data.

- **`m1.ipynb`**: Preprocessing and basic analysis of taxi trip data, including tip amounts, trip durations, and fare components.
- **`script.py`**: Inserts cleaned trip data into MongoDB and runs aggregation pipelines to:

  - Compute average tip amounts by passenger count.
  - Classify pickup and dropoff times into `morning`, `afternoon`, `evening`, and `night`.
  - Analyze popular payment types by time of day.
  - Compute trip duration and total cost, with additional support for Cassandra-style queries (commented but demonstrates cross-database logic).

### 🔁 `Mini_Project2/`

A combination of mini-projects working with Spark and Cassandra databases, focused on distributed data processing and prediction tasks.

- **`cassandra.ipynb`**: Demonstrates usage of Cassandra with CQL to query and update trip datasets, including trip duration and fare components.
- **`SparkDataframes_Songs.ipynb`**: Uses Spark DataFrames to analyze a music dataset. Operations include:

  - Filtering songs by duration or popularity.
  - Aggregating data to find top artists and genres.

- **`sparkSQL_2.ipynb`**: Demonstrates how Spark SQL is used to run complex queries on large datasets, showcasing the efficiency of in-memory distributed processing.
- **`Mobile_Price_Prediction_using_Logistic_Regression.ipynb`**: Applies Spark MLlib to build and evaluate a logistic regression model that predicts the price range of mobile phones based on features such as RAM, storage, battery life, and more.

---

## 🔧 Technologies Used

- Python (Pandas, NumPy, Matplotlib, Seaborn)
- PySpark & Spark SQL
- MongoDB + PyMongo
- Cassandra + CQL
- Jupyter Notebooks

---

## 🧠 Skills Demonstrated

- Big Data Storage & NoSQL (MongoDB, Cassandra)
- Distributed Computing (Apache Spark)
- Data Cleaning and Feature Engineering
- Exploratory Data Analysis & Visualization
- Machine Learning (Logistic Regression with Spark MLlib)
- Data Aggregation & Optimization in NoSQL

---

## 📚 Author

**Mazen Emara**
Big Data & AI Track, 2023

---
